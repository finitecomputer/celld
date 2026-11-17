// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Containers attached to Durable Objects: the engine side of
//! `ctx.container`.
//!
//! A cell of a class named in the deployment's `containers` config owns at
//! most one container, named after the cell, on the node that owns the
//! cell. The Durable Object supervises it through the ops in
//! `js/container.rs`; this module performs the effects against a container
//! engine behind the `Backend` trait: the Docker Engine API on a unix
//! socket (Podman serves the same API, `container_docker`), or a libkrun
//! microVM engine with Cloudflare's container API on its own socket
//! (`container_krun`). `CELLD_CONTAINER_ENGINE` picks one.
//!
//! The container is bound to the cell's ownership on this node, not to its
//! residency: an idle eviction leaves it running under an inactivity timer
//! and the next activation of the same cell reconnects to it by name, which
//! is what Cloudflare does and what the `@cloudflare/containers` class's
//! `sleepAfter` alarm relies on. Every other stop destroys it. The
//! container's disk is ephemeral on Cloudflare too, so a takeover that
//! starts fresh is conformant.
//!
//! Images travel through the bucket. `celld deploy` saves each image the
//! config names as a tar at `deploy/images/<id>.tar`, and a node loads it
//! into its engine the first time a cell of that class starts, so a node
//! never talks to a registry.

use crate::asyncrt;
pub use crate::container_krun::{answer_intercepted_websockets, InterceptBody, InterceptedWebSocket};
use anyhow::anyhow;
use bytes::Bytes;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// One `containers[]` entry of a deployment, as the node sees it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContainerSpec {
    pub class_name: String,
    /// The image a start that names none runs, `celld-image:<content key>`,
    /// so two deployments of one image share one tar and one load. `None`
    /// under the `durable_object` scheduling policy, whose objects name an
    /// image of `images` in `start()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// `ctx.container.images`: names to the references `start()` takes.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub images: BTreeMap<String, String>,
    /// `default` (or none) or `durable_object`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_instances: Option<u64>,
    /// The OCI runtime this class runs under, such as `runsc` (gVisor) for
    /// untrusted code. Overrides the node's `CELLD_CONTAINER_RUNTIME` for
    /// this class; `None` takes the node default. A node whose daemon does
    /// not have the runtime fails every start of the class, so the class
    /// runs only where its isolation is available. celld extends the
    /// Cloudflare config here, which has no per-class runtime. Docker only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
}

impl ContainerSpec {
    /// Every image a cell of this class may start, the default first.
    pub fn all_images(&self) -> impl Iterator<Item = &String> {
        self.image.iter().chain(self.images.values())
    }
}

/// The reference every engine holds an image under, from its content key.
/// The key hashes the layer digests and the image config, so an engine
/// that rebuilds identical content under a new id still resolves it.
pub fn image_reference(key: &str) -> String {
    format!("celld-image:{key}")
}

/// The bucket key of one saved image, from its reference.
pub fn image_key(image: &str) -> String {
    format!(
        "deploy/images/{}.tar",
        image.trim_start_matches("celld-image:")
    )
}

/// The resources an instance type reserves. Cloudflare's published table;
/// a name outside it is refused at deploy.
pub fn instance_resources(instance_type: &str) -> Option<(f64, u64)> {
    let gib = 1024 * 1024 * 1024;
    Some(match instance_type {
        "lite" | "dev" => (1.0 / 16.0, 256 * 1024 * 1024),
        "basic" => (0.25, gib),
        "standard-1" | "standard" => (0.5, 4 * gib),
        "standard-2" => (1.0, 6 * gib),
        "standard-3" => (2.0, 8 * gib),
        "standard-4" => (4.0, 12 * gib),
        _ => return None,
    })
}

/// The instance type of a class that declares none, as on Cloudflare.
const DEFAULT_INSTANCE_TYPE: &str = "dev";

/// The size a start gives its container: a named type, or `start()`'s
/// custom `{vcpu, memoryMib, diskMb}`.
#[derive(Clone, Debug, PartialEq)]
pub enum Instance {
    Named(String),
    Custom { vcpu: f64, memory_mib: u64, disk_mb: u64 },
}

impl Instance {
    /// vCPUs (a fraction for the small types) and memory in bytes.
    pub fn resources(&self) -> Option<(f64, u64)> {
        match self {
            Instance::Named(name) => instance_resources(name),
            Instance::Custom {
                vcpu, memory_mib, ..
            } => Some((*vcpu, memory_mib * 1024 * 1024)),
        }
    }

    /// The engine API's form, Cloudflare's.
    pub fn to_json(&self) -> Value {
        match self {
            // celld's alias names, Cloudflare's names to the engine.
            Instance::Named(name) => json!(match name.as_str() {
                "dev" => "lite",
                "standard" => "standard-1",
                other => other,
            }),
            Instance::Custom {
                vcpu,
                memory_mib,
                disk_mb,
            } => json!({ "vcpu": vcpu, "memoryMib": memory_mib, "diskMb": disk_mb }),
        }
    }
}

/// The memory a container of this instance type reserves on the node. A
/// class that declares none gets the default type, as a start does. An
/// unknown name is refused at deploy, so a running container always maps;
/// this returns 0 for a name that somehow does not, which undercounts
/// rather than blocks a sample.
pub fn instance_memory_bytes(instance_type: Option<&str>) -> u64 {
    instance_resources(instance_type.unwrap_or(DEFAULT_INSTANCE_TYPE))
        .map(|(_, memory)| memory)
        .unwrap_or(0)
}

/// The memory this node commits to its running containers, for the node's
/// capacity accounting. Zero when no engine has connected. See
/// `celld_logic::pressure::Load::container_reserved_bytes` for why the node
/// counts the cap rather than the container's live usage.
pub fn reserved_memory_bytes() -> u64 {
    engine_if_ready().map_or(0, |engine| engine.reserved_memory_bytes())
}

/// This node's running containers per class, published in the node lease so
/// peers can sum a class's instances across the fleet for `max_instances`.
/// Empty when no engine has connected.
pub fn running_instances_by_class() -> std::collections::BTreeMap<String, u64> {
    engine_if_ready().map_or_else(Default::default, |engine| {
        engine.running_instances_by_class()
    })
}

/// Whether `host:port` is a port this node handed `scope` for its own
/// container (`getTcpPort`), which the public-only egress policy lets the
/// object reach though it is a private address.
pub fn owns_address(scope: &str, host: &str, port: u16) -> bool {
    let Some(engine) = engine_if_ready() else {
        return false;
    };
    let Some(cell) = engine.cell(scope) else {
        return false;
    };
    let state = cell.state.lock().unwrap();
    state.running && state.address.serves(host, port)
}

/// fork: this node's running containers, by cell, for the walk down. Empty
/// when no engine has connected.
pub fn container_samples() -> Vec<celld_logic::pressure::ContainerSample> {
    engine_if_ready().map_or_else(Vec::new, |engine| engine.samples())
}

/// fork: the node's actor, which the engine tells each container's start
/// and end, and asks for room (`ContainerEngine::make_room`).
static ACTOR: OnceLock<mpsc::UnboundedSender<crate::actor::Message>> = OnceLock::new();

/// fork: links the engine to the node's actor, once, at the node's start.
pub fn set_actor(tx: mpsc::UnboundedSender<crate::actor::Message>) {
    let _ = ACTOR.set(tx);
}

/// fork: tells the core the cell's container run `run` (`next_run_id`)
/// started or stopped running, as it happens: a cell that holds one stays
/// on this node through an eviction made for room
/// (`State::holds_container`). Called with the cell's state locked, after
/// the change it tells, so what one cell tells arrives in the order it
/// happened; the run's id keeps a late stop from undoing a newer start.
fn tell_running(scope: &str, run: u64, running: bool) {
    if let Some(actor) = ACTOR.get() {
        let _ = actor.send(crate::actor::Message::ContainerRunning { cell: scope.to_string(), run, running });
    }
}

/// fork: a run's id, across every handle of this node: a handle made again
/// for a cell begins its runs at one, its ids never go back.
fn next_run_id() -> u64 {
    static IDS: AtomicU64 = AtomicU64::new(0);
    IDS.fetch_add(1, Ordering::SeqCst) + 1
}

/// fork: the engine refused a start for want of memory (krun: 429, "the
/// node's memory for VMs is spent"). The engine admits a container by its
/// instance type's whole memory, the node by what its containers use
/// (`celld_logic::pressure::container_counted_bytes`), so the engine can be
/// full while the node is not shedding.
#[derive(Debug)]
pub(crate) struct EngineFull(pub String);

impl std::fmt::Display for EngineFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EngineFull {}

/// fork: a start the engine refused for want of memory, with no idle
/// container to put to sleep for it, or refused again after one slept.
#[derive(Debug)]
pub struct NoRoomForContainer {
    pub engine: String,
    pub slept: Option<String>,
}

impl std::fmt::Display for NoRoomForContainer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.slept {
            None => write!(f, "no room for the container: the engine's memory is spent ({}), and no idle container could be put to sleep to make room", self.engine),
            Some(slept) => write!(f, "no room for the container: the engine's memory is spent ({}), even after the idle container of {slept} was put to sleep", self.engine),
        }
    }
}

impl std::error::Error for NoRoomForContainer {}

/// fork: why a container is swept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sweep {
    /// Its inactivity window ran out.
    Inactivity,
    /// A walk down short of memory put it to sleep early.
    Memory,
    /// A start the engine refused for want of memory put it to sleep.
    Room,
}

impl Sweep {
    fn as_str(self) -> &'static str {
        match self {
            Sweep::Inactivity => "inactivity",
            Sweep::Memory => "memory",
            Sweep::Room => "room",
        }
    }
}

/// How a cell stop treats its container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Release {
    /// An idle eviction: keep the container under its inactivity timer.
    Keep,
    /// Ownership leaves this node, or the cell is reset: destroy it.
    Destroy,
}

/// fork: how often the node asks an engine that reports memory in use, and
/// how long a report counts. Past it a container counts its cap again, so
/// an engine that stops answering cannot leave the node admitting on old
/// numbers.
const IN_USE_POLL: Duration = Duration::from_secs(2);
const IN_USE_FRESH_MS: u64 = 10_000;

/// fork: how long a sweep waits for the container to exit after its
/// signal to stop (SIGTERM) before it kills it: ten seconds, Docker's own
/// stop timeout. A guest is told to stop and given time to flush; an object
/// that stops its own container waits less (Fragment's computer, five
/// seconds). Cloudflare gives a container fifteen minutes at a host's
/// shutdown, which a node short of memory cannot wait.
pub(crate) const SWEEP_GRACE: Duration = Duration::from_secs(10);

/// fork: the signal a sweep sends first.
const SIGTERM: u32 = 15;

/// The default inactivity window after an idle eviction when the object
/// never set one. The `@cloudflare/containers` alarm wakes the object well
/// inside this to enforce `sleepAfter`, so this is the backstop for an
/// object that never wakes, not the policy.
const DEFAULT_INACTIVITY: Duration = Duration::from_secs(10 * 60);

/// Environment every container sees. The values mirror workerd's local
/// engine: applications read the names, never the values.
const DEFAULT_ENV: &[&str] = &[
    "CLOUDFLARE_COUNTRY_A2=XX",
    "CLOUDFLARE_DEPLOYMENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "CLOUDFLARE_LOCATION=loc01",
    "CLOUDFLARE_REGION=REGN",
    "CLOUDFLARE_APPLICATION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
];

/// Cloudflare's intercept entries per container: a hostname or a glob
/// counts two (IPv4 and IPv6), an address or a range one.
pub const INTERCEPT_ENTRIES_MAX: usize = 128;

#[derive(Clone)]
pub struct StartParams {
    /// One of the class's images; `None` takes the class's default.
    pub image: Option<String>,
    /// A snapshot id to restore (`containerSnapshot`), in place of an image.
    pub snapshot: Option<String>,
    pub instance: Option<Instance>,
    pub entrypoint: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub enable_internet: bool,
    pub labels: Vec<(String, String)>,
}

/// What a backend starts: everything decided, nothing left to look up.
pub(crate) struct Launch {
    pub scope: String,
    pub node: String,
    pub class: String,
    /// `None` when `snapshot` is the root.
    pub image: Option<String>,
    pub snapshot: Option<String>,
    /// `NAME=value`, the defaults first.
    pub env: Vec<String>,
    pub labels: Vec<(String, String)>,
    pub entrypoint: Option<Vec<String>>,
    pub enable_internet: bool,
    pub instance: Instance,
    pub runtime: Option<String>,
    pub intercepts: Vec<InterceptRule>,
}

/// How a run ended: the root process's code (137 for a kill), and whether
/// `destroy()` ended it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunExit {
    pub code: i64,
    pub destroyed: bool,
}

/// One `interceptOutbound*` rule, in the engine's terms.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct InterceptRule {
    /// `http` or `https`.
    pub scheme: &'static str,
    /// A host, a `*.` glob, `*`, an `ip:port`, or a range; an HTTPS host
    /// may end in `:port`.
    pub target: String,
}

impl InterceptRule {
    /// Its entries in Cloudflare's count of 128.
    pub fn entries(&self) -> usize {
        let host = self.target.trim_end_matches(|c: char| c.is_ascii_digit() || c == ':');
        let address = self.target.contains('/')
            || host.parse::<std::net::IpAddr>().is_ok()
            || self.target.parse::<std::net::SocketAddr>().is_ok();
        if address {
            1
        } else {
            2
        }
    }
}

/// fork: one run's outcome, kept while a live `monitor()` of it has yet to
/// read it.
#[derive(Default)]
struct Outcome {
    exit: Option<Result<RunExit, String>>,
    /// The `monitor()`s of the object's current instance that watch this
    /// run and have not read it. Those of an earlier instance went with
    /// it (`CellContainer::activate`): an outcome only they watch is
    /// abandoned.
    watchers: u32,
}

/// Where an intercepted request goes: the binding's route, as
/// `globalOutbound` keeps one, so it outlives the object's isolate.
#[derive(Clone, Debug)]
pub struct ServiceRoute {
    pub generation: crate::generation::GenerationId,
    pub script: String,
    pub entrypoint: Option<String>,
    pub props: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct Intercept {
    pub rule: InterceptRule,
    pub route: ServiceRoute,
}

/// What `inspect()` answers for the current run.
#[derive(Clone, Debug, Default)]
struct RunInfo {
    image: String,
    labels: BTreeMap<String, String>,
    from_snapshot: bool,
    started: bool,
    /// The instance type's memory: the container's cap.
    memory_bytes: u64,
    /// fork: what the engine last reported the run holding that the machine
    /// cannot take back, and when (`asyncrt::mono_ms`).
    in_use: Option<(u64, u64)>,
    /// fork: the engine's readings of the run's CPU time, what
    /// `pressure::cpu_permille` needs of them (`pressure::keep_cpu_reading`).
    cpu: std::collections::VecDeque<celld_logic::pressure::CpuReading>,
}

/// fork: what an engine reports of one running container: the memory it
/// holds that the machine cannot take back, and the CPU time it has used
/// (µs), each when the engine says.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    pub name: String,
    pub in_use_bytes: Option<u64>,
    pub cpu_usage_usec: Option<u64>,
}

#[derive(Default)]
struct CellState {
    running: bool,
    /// Where the node dials the container.
    address: Address,
    inactivity: Option<Duration>,
    /// The timer of the stop after an idle eviction, aborted when the cell
    /// returns (dropping a task handle only detaches it).
    sweeper: Option<asyncrt::TaskHandle<()>>,
    /// fork: when its cell let it go (an eviction that keeps it, monotonic
    /// ms): it runs on its inactivity timer alone. `None` while its cell
    /// holds it.
    released_mono_ms: Option<u64>,
    /// fork: its sweep, once begun (`ContainerEngine::begin_sweep`): true
    /// once that stop has ended. A cell that comes back waits for it.
    sweeping: Option<watch::Receiver<bool>>,
    /// fork: the current run's id (`next_run_id`), what `tell_running` names.
    run_id: u64,
    /// Bumped per start so a wait from a previous run cannot report for
    /// this one.
    run: u64,
    /// The run `destroy()` ended, so its exit reads as destroyed.
    destroyed: Option<u64>,
    info: RunInfo,
    /// `interceptOutbound*` so far, in order: the engine names a match by
    /// its index, and intercepts are only ever added.
    intercepts: Vec<Intercept>,
    /// fork: how many of `intercepts` (the first ones) the engine holds for
    /// this run: an `intercept()` answers once its own is among them.
    installed: usize,
    /// fork: the run whose start failed, and why: an `intercept()` that
    /// waited on that start answers with it (its rule in a launch the
    /// engine refused, or never sent).
    start_failed: Option<(u64, String)>,
}

/// Where the node reaches a container's ports.
#[derive(Clone, Default)]
pub(crate) enum Address {
    #[default]
    None,
    /// Any port at this address (Docker's bridge on Linux).
    Ip(String),
    /// Loopback ports the engine published (Docker elsewhere).
    Published(HashMap<u16, u16>),
    /// A loopback listener per port, made on first use (the krun engine).
    Forwarded(Arc<dyn PortForwarder>),
}

impl std::fmt::Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::None => f.write_str("None"),
            Address::Ip(ip) => write!(f, "Ip({ip})"),
            Address::Published(ports) => write!(f, "Published({ports:?})"),
            Address::Forwarded(_) => f.write_str("Forwarded"),
        }
    }
}

impl Address {
    fn serves(&self, host: &str, port: u16) -> bool {
        match self {
            Address::None => false,
            Address::Ip(ip) => ip == host,
            Address::Published(ports) => host == "127.0.0.1" && ports.values().any(|p| *p == port),
            Address::Forwarded(f) => host == "127.0.0.1" && f.serves(port),
        }
    }
}

/// Loopback listeners that reach a container's ports.
pub(crate) trait PortForwarder: Send + Sync {
    /// `127.0.0.1:<port>` for the container's `port`, listening.
    fn address(&self, port: u16) -> Result<String, String>;
    /// Whether this forwarder listens on loopback `port`.
    fn serves(&self, port: u16) -> bool;
    /// Stops every listener.
    fn close(&self);
}

pub struct CellContainer {
    scope: String,
    name: String,
    spec: Arc<ContainerSpec>,
    state: Mutex<CellState>,
    /// Processes `exec()` started in this container. An object drops one
    /// after `output()`; the rest go with the container.
    processes: Mutex<Vec<u64>>,
    /// The exit of the current run, `None` while it runs. `Some(Err)` is
    /// an engine failure the wait could not attribute to the process.
    /// Written with `send_replace`: a plain `send` discards the value
    /// while nobody subscribes, and `monitor()` usually subscribes late.
    exit: watch::Sender<Option<(u64, Result<RunExit, String>)>>,
    /// fork: each run's outcome, kept until the `monitor()`s that watch it
    /// have read it (`watch`, `record`, `unwatch`).
    outcomes: Mutex<BTreeMap<u64, Outcome>>,
    /// fork: the last run whose start ended: up, with the intercepts set
    /// while it was under way installed (or tried), or failed, or destroyed
    /// as it started. An `intercept()` set during a start waits for it.
    up: watch::Sender<u64>,
    /// fork: held while the intercepts are sent to the engine, each send the
    /// whole list as it is once held, so a send that read fewer never
    /// lands after one that read more (the engine replaces them whole).
    intercepts_sent: tokio::sync::Mutex<()>,
}

impl CellContainer {
    pub fn running(&self) -> bool {
        self.state.lock().unwrap().running
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `host:port` for `getTcpPort(port)`.
    pub fn address(&self, port: u16) -> Result<String, String> {
        let state = self.state.lock().unwrap();
        if !state.running {
            return Err("the container is not running".to_string());
        }
        match &state.address {
            Address::Ip(ip) => Ok(format!("{ip}:{port}")),
            Address::Published(ports) => ports
                .get(&port)
                .map(|host| format!("127.0.0.1:{host}"))
                .ok_or_else(|| {
                    format!(
                        "The container is not listening on port {port}: on this platform a port \
                         must be declared with EXPOSE in the image"
                    )
                }),
            Address::Forwarded(forwarder) => forwarder.address(port),
            Address::None => Err(format!("The container is not listening on port {port}")),
        }
    }

    pub fn set_inactivity(&self, duration: Duration) {
        self.state.lock().unwrap().inactivity = Some(duration);
    }

    /// Open the next run, synchronously, before the start is even queued.
    /// The object's `start()` returns at once and its `monitor()` follows
    /// immediately, so the run they both mean must exist before either
    /// effect runs, or the monitor would find the previous run's exit and
    /// report the new container dead on arrival.
    /// fork: what the running container counts against the node at `now`
    /// (`asyncrt::mono_ms`), `None` when it is not running.
    fn counted_memory_bytes(&self, now: u64) -> Option<u64> {
        let state = self.state.lock().unwrap();
        state.running.then(|| {
            let cap = match state.info.memory_bytes {
                0 => instance_memory_bytes(self.spec.instance_type.as_deref()),
                bytes => bytes,
            };
            let in_use = state
                .info
                .in_use
                .filter(|(_, at)| now.saturating_sub(*at) <= IN_USE_FRESH_MS)
                .map(|(bytes, _)| bytes);
            celld_logic::pressure::container_counted_bytes(cap, in_use)
        })
    }

    pub fn begin_run(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.run += 1;
        state.running = true;
        state.run_id = next_run_id();
        tell_running(&self.scope, state.run_id, true);
        state.address = Address::None;
        state.info = RunInfo::default();
        state.installed = 0;
        state.start_failed = None;
        let run = state.run;
        drop(state);
        self.exit.send_replace(None);
        self.prune_outcomes();
        run
    }

    /// The run `monitor()` waits on: the newest one.
    pub fn current_run(&self) -> u64 {
        self.state.lock().unwrap().run
    }

    /// fork: registers a `monitor()` of the current run, whose outcome is
    /// then kept until it reads it (`unwatch`): the run it means, exactly.
    pub fn watch(&self) -> u64 {
        let run = self.current_run();
        self.outcomes.lock().unwrap().entry(run).or_default().watchers += 1;
        run
    }

    /// fork: whether a `monitor()` of `run` has yet to read its outcome.
    pub fn awaited(&self, run: u64) -> bool {
        self.outcomes.lock().unwrap().get(&run).is_some_and(|outcome| outcome.watchers > 0)
    }

    /// fork: a `monitor()` of `run` has read its outcome; the outcome goes
    /// once none is left to read it.
    pub fn unwatch(&self, run: u64) {
        let mut outcomes = self.outcomes.lock().unwrap();
        if let Some(outcome) = outcomes.get_mut(&run) {
            outcome.watchers = outcome.watchers.saturating_sub(1);
            if outcome.watchers == 0 && outcome.exit.is_some() {
                outcomes.remove(&run);
            }
        }
    }

    /// fork: how run `run` ended, once it has: its exit, or `Err` for an
    /// engine failure the wait could not attribute. `None` until then. Only
    /// that run's own outcome: a later run's start never clears it, and its
    /// end never answers for it.
    pub fn ended(&self, run: u64) -> Option<Result<RunExit, String>> {
        self.outcomes.lock().unwrap().get(&run).and_then(|outcome| outcome.exit.clone())
    }

    /// fork: run `run` ended so: its outcome kept for its watchers, and the
    /// waits on the exit told.
    fn record(&self, run: u64, exit: Result<RunExit, String>) {
        self.outcomes.lock().unwrap().entry(run).or_default().exit = Some(exit.clone());
        self.exit.send_replace(Some((run, exit)));
    }

    /// fork: the object's instance is new (its `ctx.container` made): the
    /// `monitor()`s of any earlier instance went with it, so the outcomes
    /// only they watched are abandoned, and go.
    pub fn activate(&self) {
        let mut outcomes = self.outcomes.lock().unwrap();
        for outcome in outcomes.values_mut() {
            outcome.watchers = 0;
        }
        drop(outcomes);
        self.prune_outcomes();
    }

    /// fork: the ended outcomes no live watcher will read go: each read, or
    /// only abandoned watchers' (`activate`). One a live watcher waits for
    /// is never dropped. Those are bounded by the instance's `monitor()`s
    /// still unsettled: a run must end before the next starts, and the
    /// object's next event (or its exit wake) settles each ended run's.
    fn prune_outcomes(&self) {
        let mut outcomes = self.outcomes.lock().unwrap();
        outcomes.retain(|_, outcome| outcome.exit.is_none() || outcome.watchers > 0);
    }

    /// `inspect()`: the running container's image and labels, or `None`.
    /// The image is empty while it starts and when it came from a snapshot,
    /// as on Cloudflare.
    pub fn inspect(&self) -> Option<Value> {
        let state = self.state.lock().unwrap();
        if !state.running {
            return None;
        }
        let image = if state.info.started && !state.info.from_snapshot {
            state.info.image.clone()
        } else {
            String::new()
        };
        Some(json!({ "image": image, "labels": state.info.labels }))
    }

    /// `ctx.container.images`.
    pub fn images(&self) -> &BTreeMap<String, String> {
        &self.spec.images
    }
}

/// One container engine: the effects `ContainerEngine` orchestrates.
#[async_trait::async_trait]
pub(crate) trait Backend: Send + Sync {
    /// What the engine is, for messages.
    fn kind(&self) -> &'static str;
    /// Reap every container of `node` a previous process left behind.
    async fn reap(&self, node: &str) -> anyhow::Result<()>;
    /// What every start needs first (Docker: its bridges and the fence).
    async fn prepare(&self) -> anyhow::Result<()>;
    async fn has_image(&self, image: &str) -> anyhow::Result<bool>;
    /// Loads a `docker save` tar holding `image`.
    async fn load_image(&self, image: &str, tar: Bytes) -> anyhow::Result<()>;
    /// The container of that name, if it runs: where its ports are.
    async fn running(&self, name: &str) -> anyhow::Result<Option<Address>>;
    async fn create_and_start(&self, name: &str, launch: Launch) -> anyhow::Result<Address>;
    /// Waits for the root process to end.
    async fn wait(&self, name: &str) -> anyhow::Result<RunExit>;
    async fn signal(&self, name: &str, signal: u32) -> anyhow::Result<()>;
    /// Ends and removes the container; idempotent, errors ignored.
    async fn remove(&self, name: &str);
    async fn exec(&self, name: &str, params: &ExecParams) -> anyhow::Result<ExecStarted>;
    /// `snapshotContainer()`: `{id, size, name?}`.
    async fn snapshot(&self, name: &str, snapshot_name: Option<String>) -> anyhow::Result<Value>;
    /// The running container's intercepts, replaced whole (only ever grown).
    async fn set_intercepts(&self, name: &str, intercepts: &[InterceptRule]) -> anyhow::Result<()>;
    /// Whether `start()` may restore a snapshot.
    fn restores_snapshots(&self) -> bool;
    /// Whether the engine routes intercepted requests back to the object.
    fn intercepts(&self) -> bool;
    /// fork: lets go of what the backend holds outside the engine (the krun
    /// backend's intercept socket), at the node's stop.
    fn close(&self) {}

    /// fork: whether `usage` reports anything; the node polls it only then.
    fn reports_memory(&self) -> bool {
        false
    }

    /// fork: what each running container holds that the machine cannot take
    /// back, and the CPU time it has used, by container name. Empty for an
    /// engine that does not say (Docker), whose containers then count their
    /// caps and are never known to be idle.
    async fn usage(&self) -> anyhow::Result<Vec<Usage>> {
        Ok(Vec::new())
    }

    /// fork: shows run `run`'s output as the container writes it, each line
    /// an event (`container_output`), as Cloudflare shows a container's
    /// logs. The answer, called once the run has ended and awaited before
    /// its end is told, stops that, showing the last of it when asked to.
    /// `from_now` skips what the container wrote before (a container
    /// adopted from an earlier owner). `None` for an engine whose
    /// containers' output stays in its own logs (Docker's, `docker logs`).
    fn follow_output(&self, _name: &str, _cell: &str, _run: u64, _from_now: bool) -> Option<OutputEnd> {
        None
    }
}

/// fork: the end of a run's output (`Backend::follow_output`): its last
/// read when `true`.
pub(crate) type OutputEnd = Box<dyn FnOnce(bool) -> futures_util::future::BoxFuture<'static, ()> + Send>;

pub struct ContainerEngine {
    backend: Arc<dyn Backend>,
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// Per-image load lock, so two cells of one class starting together
    /// load the tar once.
    images: Mutex<HashMap<String, Arc<tokio::sync::Mutex<bool>>>>,
    cells: Mutex<HashMap<String, Arc<CellContainer>>>,
    /// fork: `SWEEP_GRACE`, shorter in a test.
    sweep_grace: Duration,
    /// fork: how long past its grace a sweep may take before what waits on
    /// it goes on (`sweep_bound`): 30 s, its signal and removal included.
    sweep_slack: Duration,
    /// fork: the actor `make_room` asks, when not the node's (`ACTOR`): a
    /// test's.
    actor: Option<mpsc::UnboundedSender<crate::actor::Message>>,
}

/// Which engine `CELLD_CONTAINER_ENGINE` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineChoice {
    /// The Docker Engine API, found as `Docker::discover` finds it.
    Docker,
    /// The krun engine's API socket (`krun:<path>`); its `ports.sock` is
    /// beside it.
    Krun(PathBuf),
    /// No engine (`none`): the node runs no container. A deployment may
    /// still declare `containers`, as a Worker written for Cloudflare does:
    /// its classes' objects activate with a `ctx.container` that never runs,
    /// whose `start()` throws "this node runs no computers", and the deploy
    /// builds no image for them.
    None,
}

impl EngineChoice {
    pub fn parse(value: Option<&str>) -> anyhow::Result<EngineChoice> {
        match value.filter(|v| !v.is_empty()) {
            None | Some("docker") => Ok(EngineChoice::Docker),
            Some("none") => Ok(EngineChoice::None),
            Some(v) => match v.strip_prefix("krun:") {
                Some(path) if path.starts_with('/') => Ok(EngineChoice::Krun(PathBuf::from(path))),
                _ => Err(anyhow!(
                    "CELLD_CONTAINER_ENGINE is `docker`, `krun:<absolute path of the engine's socket>` or `none`"
                )),
            },
        }
    }
}

/// Whether this node runs containers: `CELLD_CONTAINER_ENGINE` names an
/// engine (`docker`, the default, or `krun:`), not `none`. Read from the
/// node's configuration once `configure` has run, and from the variable
/// before (a deploy, which builds no image for a node that runs none).
pub fn runs_containers() -> bool {
    let configured = CONFIG.read().unwrap().as_ref().map(|config| config.choice.clone());
    let choice = configured.unwrap_or_else(|| {
        EngineChoice::parse(std::env::var("CELLD_CONTAINER_ENGINE").ok().as_deref()).unwrap_or(EngineChoice::Docker)
    });
    choice != EngineChoice::None
}

/// What the runtime told this module about the node: set once at start.
struct Config {
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// `CELLD_CONTAINER_RUNTIME`: the OCI runtime every container starts
    /// under, such as `runsc` (gVisor) or `kata` (a VM). `None` is the
    /// daemon's default, which is `runc` unless the operator changed it. A
    /// class can override it, see `ContainerSpec::runtime`. Docker only.
    runtime: Option<String>,
    /// `CELLD_CONTAINER_DNS`: the resolvers a container gets. Docker only;
    /// see `container_docker`.
    dns: Vec<String>,
    /// The node's state directory: the Docker engine's resolv.conf, the
    /// krun engine's intercept socket.
    data_dir: PathBuf,
    /// `CELLD_CONTAINER_ENGINE`.
    choice: EngineChoice,
    /// Where intercepted requests are dispatched.
    runtime_manager: Option<crate::runtime::RuntimeManager>,
}

static CONFIG: RwLock<Option<Config>> = RwLock::new(None);
/// The deployment's container classes. Replaced on every generation swap;
/// a running container keeps the spec it started with.
static SPECS: RwLock<Vec<Arc<ContainerSpec>>> = RwLock::new(Vec::new());
/// The deployment's `celld-fence` image; see `Manifest::fence_image`.
static FENCE_IMAGE: RwLock<Option<String>> = RwLock::new(None);
/// The engine, connected on first use. A node whose deployment declares no
/// container class never opens the socket.
static ENGINE: tokio::sync::OnceCell<Arc<ContainerEngine>> = tokio::sync::OnceCell::const_new();

pub fn configure(
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    data_dir: PathBuf,
    runtime_manager: Option<crate::runtime::RuntimeManager>,
) {
    let runtime = std::env::var("CELLD_CONTAINER_RUNTIME")
        .ok()
        .filter(|runtime| !runtime.is_empty());
    let dns = std::env::var("CELLD_CONTAINER_DNS")
        .unwrap_or_default()
        .split(',')
        .map(|resolver| resolver.trim().to_string())
        .filter(|resolver| !resolver.is_empty())
        .collect();
    let choice = match EngineChoice::parse(std::env::var("CELLD_CONTAINER_ENGINE").ok().as_deref()) {
        Ok(choice) => choice,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "containers use Docker");
            EngineChoice::Docker
        }
    };
    *CONFIG.write().unwrap() = Some(Config {
        node,
        bucket,
        runtime,
        dns,
        data_dir,
        choice,
        runtime_manager,
    });
}

pub fn install_specs(specs: Vec<ContainerSpec>, fence_image: Option<String>) {
    *SPECS.write().unwrap() = specs.into_iter().map(Arc::new).collect();
    *FENCE_IMAGE.write().unwrap() = fence_image;
}

pub fn spec(class: &str) -> Option<Arc<ContainerSpec>> {
    SPECS
        .read()
        .unwrap()
        .iter()
        .find(|spec| spec.class_name == class)
        .cloned()
}

/// The deployment's fence image, which the Docker engine runs once.
pub(crate) fn fence_image() -> Option<String> {
    FENCE_IMAGE.read().unwrap().clone()
}

/// The engine, connecting on the first call. A failure is returned rather
/// than cached, so a daemon that comes up later is found by the next call.
pub async fn engine() -> anyhow::Result<Arc<ContainerEngine>> {
    ENGINE
        .get_or_try_init(|| async {
            let (node, bucket, runtime, dns, data_dir, choice, runtime_manager) = {
                let config = CONFIG.read().unwrap();
                let config = config.as_ref().ok_or_else(|| {
                    anyhow!("the container engine is not configured on this node")
                })?;
                (
                    config.node.clone(),
                    config.bucket.clone(),
                    config.runtime.clone(),
                    config.dns.clone(),
                    config.data_dir.clone(),
                    config.choice.clone(),
                    config.runtime_manager.clone(),
                )
            };
            let backend: Arc<dyn Backend> = match choice {
                EngineChoice::Docker => {
                    let docker = crate::docker::Docker::discover().ok_or_else(|| {
                        anyhow!(
                            "no container engine: set DOCKER_HOST to a unix socket, or run a \
                             Docker or Podman daemon on this node"
                        )
                    })?;
                    Arc::new(crate::container_docker::DockerBackend::new(
                        docker, node.clone(), runtime, dns, &data_dir,
                    ))
                }
                EngineChoice::Krun(socket) => Arc::new(
                    crate::container_krun::KrunBackend::new(socket, &node, &data_dir, runtime_manager)
                        .await?,
                ),
                EngineChoice::None => anyhow::bail!("this node runs no computers"),
            };
            let engine = Arc::new(ContainerEngine::connect(backend, node, bucket).await?);
            if engine.backend.reports_memory() {
                // Ends when the engine does (the node's stop drops it).
                let weak = Arc::downgrade(&engine);
                asyncrt::spawn(async move {
                    loop {
                        asyncrt::sleep(IN_USE_POLL).await;
                        let Some(engine) = weak.upgrade() else { return };
                        engine.refresh_in_use().await;
                    }
                });
            }
            Ok(engine)
        })
        .await
        .cloned()
}

/// The engine only if a previous call connected it.
pub fn engine_if_ready() -> Option<Arc<ContainerEngine>> {
    ENGINE.get().cloned()
}

/// Destroy every container of this node at process exit. A preserve
/// shutdown (`celld dev` on Ctrl-C) keeps its cells resident and stops
/// none of them, and a handoff cut by the deadline leaves cells behind
/// too; without this their containers ran on until the next start of the
/// same node reaped them, invisible to the object that owned them.
pub async fn shutdown() {
    let Some(engine) = engine_if_ready() else {
        return;
    };
    if let Err(error) = engine.backend.reap(&engine.node).await {
        tracing::warn!(
            event = "container_shutdown_reap_failed",
            error = %format!("{error:#}"),
            "containers of this node may still be running"
        );
    }
    engine.backend.close();
}

/// fork: the internal request that wakes an object whose container's run
/// ended (`wake_cell`). The harness answers it itself (`__dispatchTo`),
/// never the object's own `fetch()`, after its event settles the
/// `monitor()` watchers whose runs ended.
pub const CONTAINER_EXITED_URL: &str = "http://container.celld.internal/exited";

/// fork: one try of an exit wake, as the object answered it.
#[derive(Debug, PartialEq)]
enum Wake {
    /// The harness answered it (204): its event settled the ended runs'
    /// watchers.
    Acknowledged,
    /// Refused or failed; tried again.
    Refused(String),
    /// The object is not on this node: its instance, and its watchers,
    /// went with it.
    Gone,
}

/// fork: how an exit wake's tries ended.
#[derive(Debug, PartialEq)]
enum Delivered {
    Acknowledged,
    Gone,
    /// No live `monitor()` of the run was left to hear it.
    NoWatcherLeft,
}

/// fork: the spacing of an exit wake's tries (a backoff doubling from
/// `first` to `max`), and how long one try may take (`each`): a try that
/// never answers is cut there and counts as refused.
#[derive(Clone, Copy)]
struct WakeTries {
    first: Duration,
    max: Duration,
    each: Duration,
}

const WAKE_TRIES: WakeTries = WakeTries {
    first: Duration::from_millis(100),
    max: Duration::from_secs(5),
    each: Duration::from_secs(10),
};

/// fork: tries `wake` until the object acknowledges it or is gone, while
/// `awaited` says a live `monitor()` of the run is left to hear it, each try
/// cut at `tries.each`. Not bounded by a count (review): a live watcher's
/// `monitor()` must settle. The watcher's own life bounds it: its object's
/// instance going ends `awaited` (`CellContainer::activate`) or leaves the
/// object gone. Answers how it ended, and how many tries it took.
async fn deliver_wake<W, F>(awaited: impl Fn() -> bool, wake: W, tries: WakeTries) -> (Delivered, u64)
where
    W: Fn() -> F,
    F: std::future::Future<Output = Wake>,
{
    let mut delay = tries.first;
    let mut tried = 0;
    // Bounded by the watcher's life, above.
    loop {
        if !awaited() {
            return (Delivered::NoWatcherLeft, tried);
        }
        tried += 1;
        match asyncrt::timeout(tries.each, wake()).await {
            Ok(Wake::Acknowledged) => return (Delivered::Acknowledged, tried),
            Ok(Wake::Gone) => return (Delivered::Gone, tried),
            Ok(Wake::Refused(why)) => tracing::debug!(tried, why = %why, "an exit wake was not acknowledged"),
            Err(_) => tracing::debug!(tried, "an exit wake had no answer within its bound"),
        }
        asyncrt::sleep(delay).await;
        delay = (delay * 2).min(tries.max);
    }
}

/// fork: wakes the object whose container's run ended, so a `monitor()` an
/// earlier event set, which could not keep that event alive, settles now,
/// as Cloudflare settles it when the container stops. The wake goes past
/// the object's request admission (`RuntimeManager::notify_cell`): its
/// handlers in flight may be the ones waiting for it. It is tried until
/// acknowledged while a live watcher of the run is left (`deliver_wake`).
async fn wake_cell(cell: &CellContainer, run: u64) {
    let runtime = CONFIG.read().unwrap().as_ref().and_then(|config| config.runtime_manager.clone());
    let Some(runtime) = runtime else {
        return;
    };
    let wake = || {
        let (runtime, scope) = (runtime.clone(), cell.scope.clone());
        async move {
            let wake = crate::engine_api::RuntimeFetch {
                url: CONTAINER_EXITED_URL.to_string(),
                method: "POST".to_string(),
                body: crate::engine_api::RequestBody::Bytes(Bytes::new()),
                headers: Vec::new(),
                request_id: None,
                order: None,
                parent: None,
            };
            match runtime.notify_cell(scope, wake).await {
                Ok(Some(answer)) if answer.status == 204 => Wake::Acknowledged,
                Ok(Some(answer)) => Wake::Refused(format!("status {}", answer.status)),
                Ok(None) => Wake::Gone,
                Err(error) => Wake::Refused(format!("{error:#}")),
            }
        }
    };
    let (delivered, tried) = deliver_wake(|| cell.awaited(run), wake, WAKE_TRIES).await;
    if tried > 1 {
        tracing::info!(cell = %cell.scope, run, tried, delivered = ?delivered, "a container's exit wake took more than one try");
    }
}

/// Connect and load every image of the installed specs, ahead of the
/// first cell that needs one. Failures are logged: a node without an
/// engine still serves every other class, and the container class fails
/// at its first `start()` with the same message.
pub async fn prewarm() {
    if !runs_containers() {
        return;
    }
    let engine = match engine().await {
        Ok(engine) => engine,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "containers are unavailable on this node");
            return;
        }
    };
    if let Err(error) = engine.backend.prepare().await {
        tracing::warn!(
            engine = engine.backend.kind(),
            error = %format!("{error:#}"),
            "the container engine is not ready; no container starts on this node"
        );
    }
    let specs = SPECS.read().unwrap().clone();
    for spec in specs {
        for image in spec.all_images() {
            if let Err(error) = engine.ensure_image(image).await {
                tracing::warn!(
                    class = %spec.class_name,
                    image = %image,
                    error = %format!("{error:#}"),
                    "container image is unavailable"
                );
            }
        }
    }
}

impl ContainerEngine {
    /// Connect to the engine and reap every container a previous process
    /// of this node left behind. A restarted node cannot know which of its
    /// containers still belong to cells it will own again, and a container
    /// whose object has lost track of it is a leak, so a restart starts
    /// clean. The next `start()` of each object creates a fresh one.
    async fn connect(
        backend: Arc<dyn Backend>,
        node: String,
        bucket: Option<crate::bucket::Bucket>,
    ) -> anyhow::Result<Self> {
        backend.reap(&node).await?;
        Ok(Self {
            backend,
            node,
            bucket,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            sweep_grace: SWEEP_GRACE,
            sweep_slack: Duration::from_secs(30),
            actor: None,
        })
    }

    /// Make an image present in the engine, loading it from the bucket when
    /// it is not. Idempotent and serialized per image.
    pub async fn ensure_image(&self, image: &str) -> anyhow::Result<()> {
        let lock = self
            .images
            .lock()
            .unwrap()
            .entry(image.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(false)))
            .clone();
        let mut loaded = lock.lock().await;
        if *loaded {
            return Ok(());
        }
        if self.backend.has_image(image).await? {
            tracing::info!(image, "container image present in the engine");
            *loaded = true;
            return Ok(());
        }
        let bucket = self
            .bucket
            .as_ref()
            .ok_or_else(|| anyhow!("image {image} is not present in the container engine"))?;
        let key = image_key(image);
        let (tar, _) = bucket.get(&key).await?.ok_or_else(|| {
            anyhow!("image {image} is not in the bucket at {key}; run `celld deploy`")
        })?;
        let bytes = tar.len();
        self.backend.load_image(image, tar).await?;
        tracing::info!(image, bytes, "container image loaded from the bucket");
        *loaded = true;
        Ok(())
    }

    /// The cell's container handle, adopting a container a previous
    /// activation on this node left running. Called at cell start for a
    /// class with a spec; the object's `running` reads the answer.
    pub async fn attach(&self, scope: &str, class: &str) -> anyhow::Result<Arc<CellContainer>> {
        let spec = spec(class).ok_or_else(|| anyhow!("class {class} has no container"))?;
        let mut waited = false;
        let cell = loop {
            let (cell, sweeping) = {
                let mut cells = self.cells.lock().unwrap();
                let cell = cells
                    .entry(scope.to_string())
                    .or_insert_with(|| {
                        Arc::new(CellContainer {
                            scope: scope.to_string(),
                            name: container_name(&self.node, scope),
                            spec: spec.clone(),
                            state: Mutex::new(CellState::default()),
                            processes: Mutex::new(Vec::new()),
                            exit: watch::channel(None).0,
                            outcomes: Mutex::new(BTreeMap::new()),
                            up: watch::channel(0).0,
                            intercepts_sent: tokio::sync::Mutex::new(()),
                        })
                    })
                    .clone();
                let mut state = cell.state.lock().unwrap();
                let sweeping = state
                    .sweeping
                    .clone()
                    .filter(|done| !waited && !*done.borrow());
                if sweeping.is_none() {
                    // A returning cell cancels the stop its eviction armed.
                    if let Some(sweeper) = state.sweeper.take() {
                        sweeper.abort();
                    }
                    state.released_mono_ms = None;
                }
                drop(state);
                (cell, sweeping)
            };
            let Some(mut done) = sweeping else { break cell };
            // fork: its sweep is under way. The cell comes back once that stop
            // has ended and taken its handle (its container gone): bounded by
            // the sweep's grace and its kill. Past that, it takes the handle
            // as it is.
            waited = true;
            let ended = asyncrt::timeout(self.sweep_bound(), async {
                while !*done.borrow_and_update() {
                    if done.changed().await.is_err() {
                        return;
                    }
                }
            })
            .await;
            if ended.is_ok() {
                waited = false;
            } else {
                tracing::warn!(scope, "container sweep still under way past its grace: the cell takes its container as it is");
            }
        };
        let running = self.backend.running(&cell.name).await?;
        // A container this handle did not start, left running by an
        // earlier owner of the name: give it a run so `monitor()` has an
        // exit to wait for. The run opens before the address lands, because
        // opening one clears it.
        let adopted = running.is_some() && !cell.running();
        if adopted {
            let run = cell.begin_run();
            {
                let mut state = cell.state.lock().unwrap();
                state.info.started = true;
                // fork: an earlier owner installed its intercepts
                state.installed = state.intercepts.len();
            }
            cell.up.send_replace(run);
            self.watch_exit(&cell, run, true);
        }
        let mut state = cell.state.lock().unwrap();
        state.running = running.is_some();
        state.address = running.unwrap_or_default();
        // a start is told as its run opens (`begin_run`); none running is
        // told here, for a handle that had one
        if !state.running {
            tell_running(&cell.scope, state.run_id, false);
        }
        drop(state);
        Ok(cell)
    }

    /// The cell's handle, if the cell started on this node.
    pub fn cell(&self, scope: &str) -> Option<Arc<CellContainer>> {
        self.cells.lock().unwrap().get(scope).cloned()
    }

    /// The cell whose container has this name.
    pub(crate) fn cell_named(&self, name: &str) -> Option<Arc<CellContainer>> {
        self.cells
            .lock()
            .unwrap()
            .values()
            .find(|cell| cell.name == name)
            .cloned()
    }

    /// The route of the intercept a container's engine named by index.
    /// fork: with the container's cell and its current run, which the node
    /// logs each intercepted request under.
    pub(crate) fn intercept_route(&self, name: &str, index: usize) -> Option<(ServiceRoute, String, u64)> {
        let cell = self.cell_named(name)?;
        let state = cell.state.lock().unwrap();
        state
            .intercepts
            .get(index)
            .map(|i| (i.route.clone(), cell.scope.clone(), state.run))
    }

    /// The memory the node's running containers reserve: each its cap, or
    /// what it uses plus room to grow once its engine has reported that
    /// within `IN_USE_FRESH_MS` (`celld_logic::pressure::container_counted_bytes`).
    /// A cell whose container is not running reserves nothing: a stopped
    /// container holds no memory.
    pub fn reserved_memory_bytes(&self) -> u64 {
        let now = asyncrt::mono_ms();
        self.cells
            .lock()
            .unwrap()
            .values()
            .filter_map(|cell| cell.counted_memory_bytes(now))
            .sum()
    }

    /// fork: asks the engine what each running container holds and keeps
    /// it on the container's current run, for `reserved_memory_bytes`.
    ///
    /// A reply answers only for the runs that were running when it was
    /// asked, and only as of when it was asked: a run that ended or was
    /// replaced (a new run under the same name) while the reply was on its
    /// way takes none of it, so an old run's measure never discounts its
    /// replacement; and the report is stamped with the time it was asked,
    /// not the time it came back, so a late reply is never fresher than it
    /// is. A reply later than `IN_USE_FRESH_MS` would be stale on arrival:
    /// the request is cut there, and a stuck one can't hold up the next.
    async fn refresh_in_use(&self) {
        let asked_at = asyncrt::mono_ms();
        let asked: HashMap<String, (Arc<CellContainer>, u64)> = self
            .cells
            .lock()
            .unwrap()
            .values()
            .filter_map(|cell| {
                let state = cell.state.lock().unwrap();
                state.running.then(|| (cell.name.clone(), (cell.clone(), state.run)))
            })
            .collect();
        let reply = asyncrt::timeout(Duration::from_millis(IN_USE_FRESH_MS), self.backend.usage()).await;
        let reports = match reply {
            Ok(Ok(reports)) => reports,
            Ok(Err(error)) => {
                tracing::debug!(error = %format!("{error:#}"), "container memory in use: no report");
                return;
            }
            Err(_) => {
                tracing::debug!("container memory in use: no report within {IN_USE_FRESH_MS} ms");
                return;
            }
        };
        for report in reports {
            let Some((cell, run)) = asked.get(&report.name) else { continue };
            let mut state = cell.state.lock().unwrap();
            if state.running && state.run == *run {
                if let Some(bytes) = report.in_use_bytes {
                    state.info.in_use = Some((bytes, asked_at));
                }
                if let Some(usage_usec) = report.cpu_usage_usec {
                    let reading = celld_logic::pressure::CpuReading { at_mono_ms: asked_at, usage_usec };
                    celld_logic::pressure::keep_cpu_reading(&mut state.info.cpu, reading);
                }
            }
        }
    }

    /// This node's running containers per class.
    fn running_instances_by_class(&self) -> std::collections::BTreeMap<String, u64> {
        let mut counts = std::collections::BTreeMap::new();
        for cell in self.cells.lock().unwrap().values() {
            if cell.running() {
                *counts.entry(cell.spec.class_name.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Refuse a start that would put the class over its fleet-wide
    /// `max_instances`. The fleet count is this node's own running
    /// containers of the class, read live, plus every peer's count from the
    /// shared capacity sample; `begin_run` already marked the starting cell
    /// running, so the live count includes it. A class with no
    /// `max_instances`, or a node with no bucket to read the sample from as
    /// under `celld dev`, has no fleet ceiling. See
    /// [`crate::ownership_store::fleet_class_instances`] for the staleness
    /// the sample admits.
    async fn enforce_instance_ceiling(&self, cell: &CellContainer) -> anyhow::Result<()> {
        let Some(max) = cell.spec.max_instances else {
            return Ok(());
        };
        let class = &cell.spec.class_name;
        let here = self
            .cells
            .lock()
            .unwrap()
            .values()
            .filter(|other| other.running() && other.spec.class_name == *class)
            .count() as u64;
        let elsewhere = match &self.bucket {
            Some(bucket) => {
                crate::ownership_store::fleet_class_instances(bucket, class, &self.node).await
            }
            None => 0,
        };
        anyhow::ensure!(
            here + elsewhere <= max,
            "container class {class} is at its max_instances limit of {max}: \
             {here} on this node and {elsewhere} on other nodes",
        );
        Ok(())
    }

    /// Start the cell's container for the run `begin_run` opened. The
    /// object's `start()` returns before this completes, as on Cloudflare;
    /// a failure surfaces through `monitor()`.
    pub async fn start(
        self: &Arc<Self>,
        cell: &Arc<CellContainer>,
        run: u64,
        params: StartParams,
    ) -> anyhow::Result<()> {
        // fork: a sweep of this cell under way (its container stopping, its
        // removal by name to come) ends before this start makes one under
        // that name: bounded by the sweep's grace and its kill
        let sweeping = cell.state.lock().unwrap().sweeping.clone();
        if let Some(mut done) = sweeping {
            let ended = asyncrt::timeout(self.sweep_bound(), async {
                while !*done.borrow_and_update() {
                    if done.changed().await.is_err() {
                        return;
                    }
                }
            })
            .await
            .is_ok();
            // never launched under its name while that removal may still
            // land: the start fails, and the object may try again
            if !ended {
                return Err(self.fail_start(cell, run, anyhow!("the container's last run is still being stopped: start() again once it has")));
            }
        }
        let again = params.clone();
        let mut started = self.create_and_start(cell, params).await;
        // fork: refused for want of memory, the start makes room: the idlest
        // container that may sleep sleeps, as a walk down puts it to sleep,
        // and the start is tried once more. With none, or refused again, it
        // fails as no room (`NoRoomForContainer`).
        let full = started
            .as_ref()
            .err()
            .and_then(|error| error.downcast_ref::<EngineFull>())
            .map(|full| full.0.clone());
        if let Some(engine) = full {
            let slept = self.make_room(cell).await;
            let ours = {
                let state = cell.state.lock().unwrap();
                state.run == run && state.destroyed != Some(run)
            };
            started = match (slept, ours) {
                (Some(slept), true) => match self.create_and_start(cell, again).await {
                    Err(error) if error.downcast_ref::<EngineFull>().is_some() => Err(NoRoomForContainer { engine, slept: Some(slept) }.into()),
                    other => other,
                },
                (slept, _) => Err(NoRoomForContainer { engine, slept }.into()),
            };
        }
        let (address, launched) = match started {
            Ok(started) => started,
            Err(error) => return Err(self.fail_start(cell, run, error)),
        };
        // `destroy()` may have come while the start was under way (`start()`
        // returns at once, and its object can destroy at once): what the
        // start made goes too, and the run ends destroyed.
        let (destroyed, added) = {
            let mut state = cell.state.lock().unwrap();
            let destroyed = state.destroyed == Some(run) || state.run != run;
            if !destroyed {
                state.running = true;
                state.address = address;
                state.info.started = true;
                state.installed = launched;
            }
            (destroyed, state.intercepts.len() > launched)
        };
        // fork: the intercepts the object set while the start was under way
        // (its `start()` returns at once, and an object sets its intercepts
        // right after, as on Cloudflare) are installed before the start
        // counts as up (`up`), which the `intercept()` that set each waits
        // for: it answers once the engine holds its rule, sending it itself
        // when this send failed, and so returns that failure.
        if !destroyed && added {
            if let Err(error) = self.send_intercepts(cell).await {
                tracing::warn!(
                    event = "container_intercepts_failed",
                    cell = %cell.scope,
                    engine = self.backend.kind(),
                    error = %format!("{error:#}"),
                    "the intercepts set during the start did not reach the engine"
                );
            }
        }
        cell.up.send_replace(run);
        if destroyed {
            self.backend.remove(&cell.name).await;
            cell.record(
                run,
                Ok(RunExit {
                    code: 137,
                    destroyed: true,
                }),
            );
            return Ok(());
        }
        self.watch_exit(cell, run, false);
        Ok(())
    }

    /// Wait for the container's root process to end and publish the exit
    /// for `run`; a later run's start has already replaced the state.
    /// fork: the run's output is shown meanwhile (`follow_output`), its last
    /// before the exit is told: the object that hears it may restart the
    /// container, and its output goes with it. Not when a later run has
    /// begun (a `destroy()`, then a `start()`): the name is that run's, and
    /// the destroyed run's output went with it. `adopted`: a container an
    /// earlier owner started, whose output so far was that owner's to show.
    fn watch_exit(&self, cell: &Arc<CellContainer>, run: u64, adopted: bool) {
        let backend = self.backend.clone();
        let cell_ = cell.clone();
        let output = backend.follow_output(&cell.name, &cell.scope, run, adopted);
        asyncrt::spawn(async move {
            let result = backend
                .wait(&cell_.name)
                .await
                .map_err(|error| format!("{error:#}"));
            if let Some(end) = output {
                let latest = cell_.state.lock().unwrap().run == run;
                end(latest).await;
            }
            let destroyed = {
                let mut state = cell_.state.lock().unwrap();
                let destroyed = state.destroyed == Some(run);
                if state.run == run {
                    state.running = false;
                    if let Address::Forwarded(forwarder) = std::mem::take(&mut state.address) {
                        forwarder.close();
                    }
                    tell_running(&cell_.scope, state.run_id, false);
                }
                destroyed
            };
            let result = result.map(|exit| RunExit {
                code: exit.code,
                destroyed: exit.destroyed || destroyed,
            });
            cell_.record(run, result);
            wake_cell(&cell_, run).await;
        })
        .detach();
    }

    /// The container's address, and how many intercepts it started with.
    async fn create_and_start(
        &self,
        cell: &CellContainer,
        params: StartParams,
    ) -> anyhow::Result<(Address, usize)> {
        self.backend.prepare().await?;
        self.enforce_instance_ceiling(cell).await?;
        let spec = &cell.spec;
        let image = match (&params.image, &params.snapshot) {
            (Some(_), Some(_)) => {
                return Err(anyhow!("start(): pass an image or a containerSnapshot, not both"))
            }
            (Some(image), None) => {
                if !spec.all_images().any(|known| known == image) {
                    return Err(anyhow!(
                        "start(): the image must be one of ctx.container.images or the class's image"
                    ));
                }
                Some(image.clone())
            }
            (None, Some(_)) => {
                if !self.backend.restores_snapshots() {
                    return Err(anyhow!(
                        "start(): containerSnapshot is not supported by celld's {} engine",
                        self.backend.kind()
                    ));
                }
                None
            }
            (None, None) => Some(spec.image.clone().ok_or_else(|| {
                anyhow!("start(): this class has the durable_object scheduling policy; pass an image")
            })?),
        };
        if let Some(image) = &image {
            self.ensure_image(image).await?;
        }
        let instance = params.instance.clone().unwrap_or_else(|| {
            Instance::Named(
                spec.instance_type
                    .clone()
                    .unwrap_or_else(|| DEFAULT_INSTANCE_TYPE.to_string()),
            )
        });
        let (_, memory_bytes) = instance
            .resources()
            .ok_or_else(|| anyhow!("start(): {instance:?} is not an instance type"))?;
        let mut env: Vec<String> = DEFAULT_ENV.iter().map(|entry| entry.to_string()).collect();
        env.push(format!("CLOUDFLARE_DURABLE_OBJECT_ID={}", cell.scope));
        env.extend(
            params
                .env
                .iter()
                .map(|(name, value)| format!("{name}={value}")),
        );
        let intercepts = {
            let mut state = cell.state.lock().unwrap();
            state.info = RunInfo {
                image: image.clone().unwrap_or_default(),
                labels: params.labels.iter().cloned().collect(),
                from_snapshot: params.snapshot.is_some(),
                started: false,
                memory_bytes,
                in_use: None,
                cpu: Default::default(),
            };
            state.intercepts.iter().map(|i| i.rule.clone()).collect::<Vec<_>>()
        };
        let launched = intercepts.len();
        // A container under a non-default runtime is Docker's matter; the
        // class names one, else the node default.
        let runtime = spec.runtime.clone();
        let address = self.backend
            .create_and_start(
                &cell.name,
                Launch {
                    scope: cell.scope.clone(),
                    node: self.node.clone(),
                    class: spec.class_name.clone(),
                    image,
                    snapshot: params.snapshot,
                    env,
                    labels: params.labels,
                    entrypoint: params.entrypoint,
                    enable_internet: params.enable_internet,
                    instance,
                    runtime,
                    intercepts,
                },
            )
            .await?;
        Ok((address, launched))
    }

    /// Wait for run `run` to end. fork: that run's own outcome, which a
    /// later start never clears (`CellContainer::record`).
    pub async fn monitor(&self, cell: &Arc<CellContainer>, run: u64) -> Result<RunExit, String> {
        let mut receiver = cell.exit.subscribe();
        loop {
            if let Some(result) = cell.ended(run) {
                return result;
            }
            if receiver.changed().await.is_err() {
                return Err("the container engine went away".to_string());
            }
        }
    }

    /// Run `run`'s start failed: its end, and the error to return.
    fn fail_start(&self, cell: &Arc<CellContainer>, run: u64, error: anyhow::Error) -> anyhow::Error {
        // `start()` is fire-and-forget for the object, and its `monitor()`
        // sees this error only if it was already waiting; say it here too,
        // or a start that fails fast leaves no trace anywhere.
        tracing::warn!(
            event = "container_start_failed",
            cell = %cell.scope,
            engine = self.backend.kind(),
            error = %format!("{error:#}"),
            "the container did not start"
        );
        let mut state = cell.state.lock().unwrap();
        // fork: of this run only: a later one's state is its own
        if state.run == run {
            state.running = false;
            state.start_failed = Some((run, format!("{error:#}")));
            tell_running(&cell.scope, state.run_id, false);
        }
        drop(state);
        cell.record(run, Err(format!("{error:#}")));
        cell.up.send_replace(run);
        error
    }

    pub async fn destroy(&self, cell: &Arc<CellContainer>) -> anyhow::Result<()> {
        {
            let mut state = cell.state.lock().unwrap();
            state.destroyed = Some(state.run);
        }
        self.backend.remove(&cell.name).await;
        let mut state = cell.state.lock().unwrap();
        state.running = false;
        if let Address::Forwarded(forwarder) = std::mem::take(&mut state.address) {
            forwarder.close();
        }
        tell_running(&cell.scope, state.run_id, false);
        Ok(())
    }

    pub async fn signal(&self, cell: &Arc<CellContainer>, signal: u32) -> anyhow::Result<()> {
        self.backend.signal(&cell.name, signal).await
    }

    /// `snapshotContainer({name})`.
    pub async fn snapshot(&self, cell: &Arc<CellContainer>, name: Option<String>) -> anyhow::Result<Value> {
        if !cell.running() {
            return Err(anyhow!("snapshotContainer() requires a running container."));
        }
        self.backend.snapshot(&cell.name, name).await
    }

    /// `interceptOutbound*`: added for the cell's container's life, and to a
    /// running container at once. Cloudflare's count of 128 entries holds.
    /// fork: a rule the container has already (its scheme and target) takes
    /// the new binding in its place, as on Cloudflare, where the Sandbox
    /// SDK's `DirectoryBackup` re-registers its host for each grant and
    /// deny; adding it again grew the list to the cap, past which every
    /// intercept was refused. Its index, which the engine names, is kept.
    pub async fn intercept(
        &self,
        cell: &Arc<CellContainer>,
        rule: InterceptRule,
        route: ServiceRoute,
    ) -> anyhow::Result<()> {
        // Refused before it is recorded: a rule kept for an engine that
        // cannot route it would refuse every later start.
        anyhow::ensure!(
            self.backend.intercepts(),
            "interceptOutboundHttp() is not supported by celld's {} engine",
            self.backend.kind()
        );
        let (index, waited) = {
            let mut state = cell.state.lock().unwrap();
            let index = match state.intercepts.iter().position(|i| i.rule == rule) {
                Some(index) => {
                    state.intercepts[index].route = route;
                    index
                }
                None => {
                    let used: usize = state.intercepts.iter().map(|i| i.rule.entries()).sum();
                    anyhow::ensure!(
                        used + rule.entries() <= INTERCEPT_ENTRIES_MAX,
                        "a container takes at most {INTERCEPT_ENTRIES_MAX} intercept entries (a hostname counts two)"
                    );
                    state.intercepts.push(Intercept { rule, route });
                    state.intercepts.len() - 1
                }
            };
            // the start under way, if one is: this rule is its to install,
            // in its launch or after it
            let starting = state.running && *cell.up.borrow() < state.run;
            (index, starting.then_some(state.run))
        };
        // fork: answered once the engine holds the rule, never before: a
        // start under way is waited out (it installs what was set meanwhile,
        // in its launch or after it, before it counts as up). A start that
        // failed is the error of each registration that waited on it; a
        // rule a start did not install is sent here, its failure returned.
        // A container that is not running holds none, and no start was
        // asked to install the rule: its next start takes it with the rest.
        // Bounded by the start, which ends either way (`up`).
        let mut up = cell.up.subscribe();
        loop {
            let starting = {
                let state = cell.state.lock().unwrap();
                if state.installed > index {
                    return Ok(());
                }
                if !state.running {
                    return match (waited, &state.start_failed) {
                        (Some(run), Some((failed, error))) if *failed == run => {
                            Err(anyhow!("the start this intercept waited on failed: {error}"))
                        }
                        _ => Ok(()),
                    };
                }
                *up.borrow_and_update() < state.run
            };
            if !starting {
                return self.send_intercepts(cell).await;
            }
            if up.changed().await.is_err() {
                return Err(anyhow!("the container engine went away"));
            }
        }
    }

    /// fork: the container's intercepts, all of them as they are once the
    /// send is held, to the engine, when it runs; a start under way sends
    /// what was added meanwhile itself (`start`).
    async fn send_intercepts(&self, cell: &CellContainer) -> anyhow::Result<()> {
        let _held = cell.intercepts_sent.lock().await;
        let rules = {
            let state = cell.state.lock().unwrap();
            let running = state.running && state.info.started;
            running.then(|| (state.run, state.intercepts.iter().map(|i| i.rule.clone()).collect::<Vec<_>>()))
        };
        if let Some((run, rules)) = rules {
            self.backend.set_intercepts(&cell.name, &rules).await?;
            let mut state = cell.state.lock().unwrap();
            if state.run == run {
                state.installed = state.installed.max(rules.len());
            }
        }
        Ok(())
    }

    /// The cell left this node's runtime. An idle eviction keeps the
    /// container for its inactivity window; anything else destroys it, and
    /// the stop waits for that: a drain ends the process right after its
    /// last stop, and a destroy left to a detached task did not always get
    /// its two daemon calls in before the exit.
    pub async fn release(self: &Arc<Self>, scope: &str, release: Release) {
        let Some(cell) = self.cell(scope) else {
            return;
        };
        match release {
            Release::Keep => {
                let window = {
                    let mut state = cell.state.lock().unwrap();
                    if state.sweeping.is_some() {
                        return;
                    }
                    state.released_mono_ms = Some(asyncrt::mono_ms());
                    state.inactivity.unwrap_or(DEFAULT_INACTIVITY)
                };
                let engine = self.clone();
                let cell_ = cell.clone();
                let sweeper = asyncrt::spawn(async move {
                    asyncrt::sleep(window).await;
                    engine.begin_sweep(&cell_, Sweep::Inactivity);
                });
                if let Some(previous) = cell.state.lock().unwrap().sweeper.replace(sweeper) {
                    previous.abort();
                }
            }
            Release::Destroy => self.forget(&cell).await,
        }
    }

    /// fork: how long what waits on a sweep waits: its grace, then its slack.
    fn sweep_bound(&self) -> Duration {
        self.sweep_grace + self.sweep_slack
    }

    /// fork: makes room for `starting`, whose start the engine refused for
    /// want of memory: the actor puts the idlest container that may sleep now
    /// to sleep (`Message::MakeRoom`; never `starting`'s own), and this waits
    /// for its stop to end, so its memory is the engine's again. The cell
    /// whose container slept, or `None` when none could.
    async fn make_room(self: &Arc<Self>, starting: &CellContainer) -> Option<String> {
        // the actor chooses, as of its step: which cells are at work, and
        // which did anything lately, are the core's to know as they are now
        let actor = self.actor.clone().or_else(|| ACTOR.get().cloned())?;
        let (reply, answer) = tokio::sync::oneshot::channel();
        actor.send(crate::actor::Message::MakeRoom { starting: starting.scope.clone(), reply }).ok()?;
        let scope = answer.await.ok().flatten()?;
        tracing::info!(
            event = "container_slept_for_memory",
            scope = %scope,
            why = "a start the engine refused",
            starting = %starting.scope,
            "idle container put to sleep for memory"
        );
        let waits = self.cell(&scope).and_then(|cell| cell.state.lock().unwrap().sweeping.clone());
        if let Some(mut done) = waits {
            let _ = asyncrt::timeout(self.sweep_bound(), async {
                while !*done.borrow_and_update() {
                    if done.changed().await.is_err() {
                        return;
                    }
                }
            })
            .await;
        }
        Some(scope)
    }

    /// fork: the actor's answer to `make_room`: the first of `candidates`
    /// (the core's sleepable containers, the idlest first) that this engine
    /// puts to sleep now, if any.
    pub fn sleep_one_for_room(self: &Arc<Self>, candidates: &[String]) -> Option<String> {
        candidates
            .iter()
            .find(|scope| self.cell(scope).is_some_and(|cell| self.begin_sleep(&cell, Sweep::Room)))
            .cloned()
    }

    /// fork: puts the container of the cell `scope` to sleep now, for
    /// memory, as the core chose it (its object at no work: no request, no
    /// socket, idle past its window): the sweep its inactivity timer would
    /// begin, early, when its cell let it go; else the same stop, its cell
    /// keeping the handle, whose `monitor()` sees the run end. Once: false
    /// when it is not running or already stopping.
    pub fn sleep_for_memory(self: &Arc<Self>, scope: &str) -> bool {
        self.cell(scope)
            .is_some_and(|cell| self.begin_sleep(&cell, Sweep::Memory))
    }

    /// fork: `sleep_for_memory`'s stop, for `why`.
    fn begin_sleep(self: &Arc<Self>, cell: &Arc<CellContainer>, why: Sweep) -> bool {
        if cell.state.lock().unwrap().released_mono_ms.is_some() {
            return self.begin_sweep(cell, why);
        }
        let (run, done) = {
            let mut state = cell.state.lock().unwrap();
            if !state.running || state.sweeping.is_some() || state.released_mono_ms.is_some() {
                return false;
            }
            let (done, waits) = watch::channel(false);
            state.sweeping = Some(waits);
            (state.run, done)
        };
        let engine = self.clone();
        let cell = cell.clone();
        asyncrt::spawn(async move {
            engine.stop_gracefully(&cell, run, why).await;
            // its cell keeps the handle: a stop it can follow, then none
            cell.state.lock().unwrap().sweeping = None;
            done.send_replace(true);
        })
        .detach();
        true
    }

    /// fork: begins the cell's sweep, once, if its cell let it go: the stop
    /// `sleepAfter` makes (`stop_gracefully`), then its handle goes. A cell
    /// that comes back meanwhile waits for it (`attach`). The timer that
    /// armed it is spent: aborted when the sweep is early.
    fn begin_sweep(self: &Arc<Self>, cell: &Arc<CellContainer>, why: Sweep) -> bool {
        let (run, running, done) = {
            let mut state = cell.state.lock().unwrap();
            if state.released_mono_ms.is_none() || state.sweeping.is_some() {
                return false;
            }
            let (done, waits) = watch::channel(false);
            state.sweeping = Some(waits);
            if let Some(timer) = state.sweeper.take() {
                // the timer calls this at its end: never abort that call
                if why != Sweep::Inactivity {
                    timer.abort();
                }
            }
            (state.run, state.running, done)
        };
        let engine = self.clone();
        let cell = cell.clone();
        asyncrt::spawn(async move {
            if running {
                // its stop ends with its removal
                engine.stop_gracefully(&cell, run, why).await;
            } else if cell.state.lock().unwrap().released_mono_ms.is_some() {
                // none running: what the engine may keep of it goes (a start
                // of a run its cell took back waits for this sweep)
                engine.backend.remove(&cell.name).await;
            }
            // the handle goes, unless its cell took it back meanwhile (an
            // `attach` that waited past the sweep's bound): checked and
            // taken under the locks `attach` takes, in its order, so the two
            // never both have it; one taken back is swept no more
            let gone = {
                let mut cells = engine.cells.lock().unwrap();
                let mut state = cell.state.lock().unwrap();
                if state.released_mono_ms.is_some() {
                    if cells.get(&cell.scope).is_some_and(|current| Arc::ptr_eq(current, &cell)) {
                        cells.remove(&cell.scope);
                    }
                    true
                } else {
                    state.sweeping = None;
                    false
                }
            };
            if gone {
                for id in cell.processes.lock().unwrap().drain(..) {
                    drop_process(id);
                }
            }
            done.send_replace(true);
        })
        .detach();
        true
    }

    /// fork: the stop of a sweep, as Cloudflare's `sleepAfter` makes it: the
    /// signal to stop (SIGTERM), up to `sweep_grace` for run `run` to exit,
    /// then the kill (`destroy`) unless a later run began. The object's
    /// `monitor()` sees the run end as it ended: its own exit, or the kill's.
    async fn stop_gracefully(&self, cell: &Arc<CellContainer>, run: u64, why: Sweep) {
        let t0 = asyncrt::mono_ms();
        let signalled = self.backend.signal(&cell.name, SIGTERM).await;
        let mut ends = cell.exit.subscribe();
        let ended = |ends: &watch::Receiver<Option<(u64, Result<RunExit, String>)>>| {
            cell.current_run() != run
                || cell.ended(run).is_some()
                || matches!(&*ends.borrow(), Some((ended, _)) if *ended == run)
        };
        let exited = asyncrt::timeout(self.sweep_grace, async {
            while !ended(&ends) {
                if ends.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .is_ok();
        let killed = !exited && cell.current_run() == run && cell.running();
        // the kill, or the removal of what exited: of run `run` alone. A
        // start after it waits for this sweep (`start`), so the removal by
        // name never meets a later run's container; and what it sets after
        // is that run's only.
        let ours = {
            let mut state = cell.state.lock().unwrap();
            let ours = state.run == run;
            if ours {
                state.destroyed = Some(run);
            }
            ours
        };
        if ours {
            self.backend.remove(&cell.name).await;
            let mut state = cell.state.lock().unwrap();
            if state.run == run {
                state.running = false;
                if let Address::Forwarded(forwarder) = std::mem::take(&mut state.address) {
                    forwarder.close();
                }
                tell_running(&cell.scope, state.run_id, false);
            }
        }
        tracing::info!(
            event = "container_swept",
            scope = %cell.scope,
            run,
            why = why.as_str(),
            signalled = signalled.is_ok(),
            exited,
            killed,
            ms = asyncrt::mono_ms().saturating_sub(t0),
            "container stopped: signalled to stop, then removed"
        );
    }

    /// fork: each running container, by its cell, for the walk down.
    pub fn samples(&self) -> Vec<celld_logic::pressure::ContainerSample> {
        let now = asyncrt::mono_ms();
        // an engine that reports what containers use: one it has not
        // reported yet counts its cap provisionally
        let reports = self.backend.reports_memory();
        self.cells
            .lock()
            .unwrap()
            .values()
            .filter_map(|cell| {
                let counted_bytes = cell.counted_memory_bytes(now)?;
                let state = cell.state.lock().unwrap();
                let reported = state.info.in_use.is_some_and(|(_, at)| now.saturating_sub(at) <= IN_USE_FRESH_MS);
                let readings: Vec<_> = state.info.cpu.iter().copied().collect();
                Some(celld_logic::pressure::ContainerSample {
                    cell: cell.scope.clone(),
                    counted_bytes,
                    released_mono_ms: state.released_mono_ms,
                    stopping: state.sweeping.is_some(),
                    provisional: reports && !reported,
                    cpu_permille: celld_logic::pressure::cpu_permille(&readings, now),
                })
            })
            .collect()
    }

    async fn forget(&self, cell: &Arc<CellContainer>) {
        let _ = self.destroy(cell).await;
        self.let_go(cell);
    }

    /// The handle goes, and the processes it ran with it.
    fn let_go(&self, cell: &Arc<CellContainer>) {
        for id in cell.processes.lock().unwrap().drain(..) {
            drop_process(id);
        }
        let mut cells = self.cells.lock().unwrap();
        if cells
            .get(&cell.scope)
            .is_some_and(|current| Arc::ptr_eq(current, cell))
        {
            cells.remove(&cell.scope);
        }
    }

    pub async fn exec(
        &self,
        cell: &Arc<CellContainer>,
        params: ExecParams,
    ) -> anyhow::Result<Arc<ExecProcess>> {
        // fork: an exec while the start is under way waits for it, as on
        // Cloudflare, where exec() on a container still starting runs once it
        // is up; the krun engine refuses one (409, still starting). Up is the
        // start's end (`up`), after it installed the intercepts set while it
        // ran, never its container's start alone, or an exec could reach a
        // host before its intercept. Bounded by the start, which ends either
        // way.
        let mut up = cell.up.subscribe();
        loop {
            {
                let state = cell.state.lock().unwrap();
                if !state.running {
                    return Err(anyhow!("exec() requires a running container."));
                }
                if *up.borrow_and_update() >= state.run {
                    break;
                }
            }
            if up.changed().await.is_err() {
                return Err(anyhow!("the container engine went away"));
            }
        }
        let pty = params.pty.is_some();
        let started = self.backend.exec(&cell.name, &params).await?;
        let process = Arc::new(ExecProcess {
            id: next_exec_id(),
            pid: started.pid,
            pty,
            io: started.io,
            stdout: tokio::sync::Mutex::new(started.stdout),
            stderr: tokio::sync::Mutex::new(started.stderr),
            exit_code: tokio::sync::Mutex::new(None),
        });
        processes()
            .lock()
            .unwrap()
            .insert(process.id, process.clone());
        cell.processes.lock().unwrap().push(process.id);
        Ok(process)
    }
}

/// Where an exec's output goes, as `exec()`'s options say.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputMode {
    Pipe,
    Ignore,
    /// stderr only: folded into stdout.
    Combined,
}

pub struct ExecParams {
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    pub stdin: bool,
    pub stdout: OutputMode,
    pub stderr: OutputMode,
    /// A pseudo-terminal of `(cols, rows)`.
    pub pty: Option<(u16, u16)>,
}

/// An exec's running half, the engine's: its input, its end, its signals.
#[async_trait::async_trait]
pub(crate) trait ExecIo: Send + Sync {
    async fn write(&self, bytes: &[u8]) -> Result<(), String>;
    async fn close_stdin(&self);
    /// The exit code (128 + n for a signal).
    async fn wait(&self) -> anyhow::Result<i64>;
    async fn kill(&self, signal: u32) -> anyhow::Result<()>;
    async fn resize(&self, cols: u16, rows: u16) -> anyhow::Result<()>;
}

/// What a backend's `exec` returns once the process has started.
pub(crate) struct ExecStarted {
    pub pid: i64,
    pub io: Box<dyn ExecIo>,
    pub stdout: mpsc::Receiver<Bytes>,
    pub stderr: mpsc::Receiver<Bytes>,
}

pub struct ExecProcess {
    pub id: u64,
    pub pid: i64,
    pub pty: bool,
    io: Box<dyn ExecIo>,
    stdout: tokio::sync::Mutex<mpsc::Receiver<Bytes>>,
    stderr: tokio::sync::Mutex<mpsc::Receiver<Bytes>>,
    exit_code: tokio::sync::Mutex<Option<i64>>,
}

fn processes() -> &'static Mutex<HashMap<u64, Arc<ExecProcess>>> {
    static PROCESSES: OnceLock<Mutex<HashMap<u64, Arc<ExecProcess>>>> = OnceLock::new();
    PROCESSES.get_or_init(Default::default)
}

fn next_exec_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

pub fn process(id: u64) -> Option<Arc<ExecProcess>> {
    processes().lock().unwrap().get(&id).cloned()
}

pub fn drop_process(id: u64) {
    processes().lock().unwrap().remove(&id);
}

impl ExecProcess {
    /// One chunk of stdout (`1`) or stderr (`2`); empty at end of stream.
    pub async fn read(&self, which: u8) -> Bytes {
        let mut receiver = match which {
            1 => self.stdout.lock().await,
            _ => self.stderr.lock().await,
        };
        receiver.recv().await.unwrap_or_default()
    }

    pub async fn write(&self, bytes: &[u8]) -> Result<(), String> {
        self.io.write(bytes).await
    }

    pub async fn close_stdin(&self) {
        self.io.close_stdin().await;
    }

    pub async fn wait(&self) -> anyhow::Result<i64> {
        let mut exit = self.exit_code.lock().await;
        if let Some(code) = *exit {
            return Ok(code);
        }
        let code = self.io.wait().await?;
        *exit = Some(code);
        Ok(code)
    }

    pub async fn kill(&self, signal: u32) -> anyhow::Result<()> {
        self.io.kill(signal).await
    }

    pub async fn resize(&self, cols: u16, rows: u16) -> anyhow::Result<()> {
        if !self.pty {
            return Err(anyhow!("resize() requires a process started with pty"));
        }
        self.io.resize(cols, rows).await
    }
}

/// A container name from a node and a cell scope: the scope's characters
/// are not all legal, so the name is a hash and the scope rides in a label
/// (Docker) or in celld's map (krun). The node is part of it because two
/// nodes can share one engine in a development or test setup, and a name
/// from the scope alone let one node adopt, or reap, the other's container
/// for the same object; its own tag up front lets a node reap its own by
/// prefix.
pub(crate) fn container_name(node: &str, scope: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(format!("{node}\n{scope}").as_bytes());
    format!("{}{:x}", node_prefix(node), digest)[..30].to_string()
}

/// The start of every container name of `node`.
pub(crate) fn node_prefix(node: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(node.as_bytes());
    format!("celld-{}-", &format!("{digest:x}")[..6])
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    /// A point the fake engine holds a step at, when told to, until the
    /// test opens it: what forces an interleaving instead of racing for it.
    struct Gate {
        hold: AtomicBool,
        entered: tokio::sync::Notify,
        open: watch::Sender<bool>,
    }

    impl Default for Gate {
        fn default() -> Gate {
            Gate { hold: AtomicBool::new(false), entered: tokio::sync::Notify::new(), open: watch::channel(false).0 }
        }
    }

    impl Gate {
        fn hold(&self) {
            self.hold.store(true, Ordering::SeqCst);
        }

        async fn pass(&self) {
            if !self.hold.load(Ordering::SeqCst) {
                return;
            }
            self.entered.notify_one();
            let mut open = self.open.subscribe();
            while !*open.borrow_and_update() {
                open.changed().await.unwrap();
            }
        }

        /// Until the held step is reached.
        async fn reached(&self) {
            self.entered.notified().await;
        }

        fn open(&self) {
            self.open.send_replace(true);
        }
    }

    /// The fake engine: a start's image preparation (`has_image`) and its
    /// launch (`create_and_start`) can be held; a target can be refused,
    /// at launch and at install alike; an install can be slow; and each
    /// exec records the rules the engine held as it ran.
    #[derive(Default)]
    struct Fake {
        image: Gate,
        launch: Gate,
        install: Gate,
        refused: Mutex<Option<String>>,
        install_ms: AtomicU64,
        launched: Mutex<Vec<InterceptRule>>,
        installed: Mutex<Vec<InterceptRule>>,
        seen_by_exec: Mutex<Vec<Vec<String>>>,
        in_use: Mutex<Vec<(String, u64)>>,
        /// The CPU time each container has used (µs), by name.
        cpu: Mutex<BTreeMap<String, u64>>,
        /// Runs once a report is sampled, before it is answered: what
        /// happens to a container while the report is on its way.
        in_use_meanwhile: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        /// How long a sampled report takes to be answered.
        in_use_delay: Mutex<Option<std::time::Duration>>,
        /// Whether a run ends at all: off, `wait` never answers, as before.
        ends: AtomicBool,
        /// Whether the guest exits on its own at SIGTERM (code 0); otherwise
        /// only the removal ends it (137).
        exits_at_term: AtomicBool,
        /// Each start, refusal, signal and removal asked of a container:
        /// what, its name, and when (`asyncrt::mono_ms`).
        asked: Mutex<Vec<(String, String, u64)>>,
        /// Each container's end, once it came: what `wait` answers.
        exit: Mutex<HashMap<String, i64>>,
        exited: tokio::sync::Notify,
        /// How many starts from now on the engine refuses for want of
        /// memory, as krun's 429 does.
        full: AtomicU64,
        /// A removal, held when told to.
        removal: Gate,
    }

    impl Fake {
        fn end(&self, name: &str, code: i64) {
            let mut exit = self.exit.lock().unwrap();
            if !exit.contains_key(name) {
                exit.insert(name.to_string(), code);
                self.exited.notify_waiters();
            }
        }

        fn ask(&self, what: &str, name: &str) {
            self.asked.lock().unwrap().push((what.to_string(), name.to_string(), asyncrt::mono_ms()));
        }

        /// What was asked of the container `name`, in order.
        fn asked_of(&self, name: &str) -> Vec<String> {
            self.asked.lock().unwrap().iter().filter(|(_, n, _)| n == name).map(|(what, ..)| what.clone()).collect()
        }
    }

    impl Fake {
        fn refuses(&self, rules: &[InterceptRule]) -> Option<String> {
            let refused = self.refused.lock().unwrap().clone()?;
            rules.iter().any(|r| r.target == refused).then(|| format!("not a host: {refused}"))
        }
    }

    struct NoIo;

    #[async_trait::async_trait]
    impl ExecIo for NoIo {
        async fn write(&self, _: &[u8]) -> Result<(), String> {
            Ok(())
        }
        async fn close_stdin(&self) {}
        async fn wait(&self) -> anyhow::Result<i64> {
            Ok(0)
        }
        async fn kill(&self, _: u32) -> anyhow::Result<()> {
            Ok(())
        }
        async fn resize(&self, _: u16, _: u16) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Backend for Fake {
        fn kind(&self) -> &'static str {
            "fake"
        }
        async fn reap(&self, _: &str) -> anyhow::Result<()> {
            Ok(())
        }
        async fn prepare(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn has_image(&self, _: &str) -> anyhow::Result<bool> {
            self.image.pass().await;
            Ok(true)
        }
        async fn load_image(&self, _: &str, _: Bytes) -> anyhow::Result<()> {
            Ok(())
        }
        async fn running(&self, _: &str) -> anyhow::Result<Option<Address>> {
            Ok(None)
        }
        async fn create_and_start(&self, name: &str, launch: Launch) -> anyhow::Result<Address> {
            if self.full.load(Ordering::SeqCst) > 0 {
                self.full.fetch_sub(1, Ordering::SeqCst);
                self.ask("refused", name);
                return Err(EngineFull("start container failed with [429] the node's memory for VMs is spent (30720 of 32768 MiB)".into()).into());
            }
            self.ask("start", name);
            self.exit.lock().unwrap().remove(name);
            *self.launched.lock().unwrap() = launch.intercepts.clone();
            self.launch.pass().await;
            if let Some(refused) = self.refuses(&launch.intercepts) {
                return Err(anyhow!("start container failed with [400] {refused}"));
            }
            *self.installed.lock().unwrap() = launch.intercepts;
            Ok(Address::None)
        }
        async fn wait(&self, name: &str) -> anyhow::Result<RunExit> {
            if !self.ends.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            loop {
                let notified = self.exited.notified();
                if let Some(code) = self.exit.lock().unwrap().get(name) {
                    return Ok(RunExit { code: *code, destroyed: false });
                }
                notified.await;
            }
        }
        async fn signal(&self, name: &str, signal: u32) -> anyhow::Result<()> {
            self.ask(&format!("signal {signal}"), name);
            if signal == SIGTERM && self.exits_at_term.load(Ordering::SeqCst) {
                self.end(name, 0);
            }
            Ok(())
        }
        async fn remove(&self, name: &str) {
            self.removal.pass().await;
            self.ask("remove", name);
            self.end(name, 137);
        }
        async fn exec(&self, _: &str, _: &ExecParams) -> anyhow::Result<ExecStarted> {
            let seen = self.installed.lock().unwrap().iter().map(|r| r.target.clone()).collect();
            self.seen_by_exec.lock().unwrap().push(seen);
            Ok(ExecStarted { pid: 1, io: Box::new(NoIo), stdout: mpsc::channel(1).1, stderr: mpsc::channel(1).1 })
        }
        async fn snapshot(&self, _: &str, _: Option<String>) -> anyhow::Result<Value> {
            Ok(Value::Null)
        }
        async fn set_intercepts(&self, _: &str, intercepts: &[InterceptRule]) -> anyhow::Result<()> {
            self.install.pass().await;
            asyncrt::sleep(Duration::from_millis(self.install_ms.load(Ordering::SeqCst))).await;
            if let Some(refused) = self.refuses(intercepts) {
                return Err(anyhow!("intercepts failed with [400] {refused}"));
            }
            *self.installed.lock().unwrap() = intercepts.to_vec();
            Ok(())
        }
        fn reports_memory(&self) -> bool {
            true
        }
        async fn usage(&self) -> anyhow::Result<Vec<Usage>> {
            let cpu = self.cpu.lock().unwrap().clone();
            let mut report: Vec<Usage> = self
                .in_use
                .lock()
                .unwrap()
                .iter()
                .map(|(name, bytes)| Usage { name: name.clone(), in_use_bytes: Some(*bytes), cpu_usage_usec: cpu.get(name).copied() })
                .collect();
            for (name, usec) in &cpu {
                if !report.iter().any(|u| u.name == *name) {
                    report.push(Usage { name: name.clone(), in_use_bytes: None, cpu_usage_usec: Some(*usec) });
                }
            }
            if let Some(meanwhile) = self.in_use_meanwhile.lock().unwrap().take() {
                meanwhile();
            }
            let delay = *self.in_use_delay.lock().unwrap();
            if let Some(delay) = delay {
                asyncrt::sleep(delay).await;
            }
            Ok(report)
        }
        fn restores_snapshots(&self) -> bool {
            false
        }
        fn intercepts(&self) -> bool {
            true
        }
    }

    /// The runtime the engine's tasks run on in these tests (asyncrt takes
    /// one process handle, once).
    pub(crate) fn rt() -> &'static tokio::runtime::Runtime {
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
            asyncrt::set_host_handle(runtime.handle().clone());
            runtime
        })
    }

    /// A cell of class `Box`, whose one image is `img`.
    fn a_box() -> Arc<CellContainer> {
        Arc::new(CellContainer {
            scope: "Box:1".into(),
            name: container_name("node-a", "Box:1"),
            spec: Arc::new(serde_json::from_value(json!({ "class_name": "Box", "images": { "base": "img" } })).unwrap()),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            exit: watch::channel(None).0,
            outcomes: Mutex::new(BTreeMap::new()),
            up: watch::channel(0).0,
            intercepts_sent: tokio::sync::Mutex::new(()),
        })
    }

    async fn an_engine(fake: &Arc<Fake>) -> Arc<ContainerEngine> {
        Arc::new(ContainerEngine::connect(fake.clone(), "node-a".into(), None).await.unwrap())
    }

    fn start_img() -> StartParams {
        StartParams {
            image: Some("img".into()),
            snapshot: None,
            instance: None,
            entrypoint: None,
            env: Vec::new(),
            enable_internet: false,
            labels: Vec::new(),
        }
    }

    fn http(target: &str) -> InterceptRule {
        InterceptRule { scheme: "http", target: target.into() }
    }

    fn a_route() -> ServiceRoute {
        ServiceRoute { generation: 1, script: "worker".into(), entrypoint: None, props: Vec::new() }
    }

    /// Until `cell` has recorded `n` intercepts, a few seconds at most.
    async fn recorded(cell: &CellContainer, n: usize) {
        for _ in 0..5000 {
            if cell.state.lock().unwrap().intercepts.len() >= n {
                return;
            }
            asyncrt::sleep(Duration::from_millis(1)).await;
        }
        panic!("{n} intercepts were never recorded");
    }

    // Goal: a running container counts its cap until the engine reports
    // what it uses, then that plus a quarter of its cap; a report older
    // than IN_USE_FRESH_MS counts the cap again, a new run starts from its
    // cap, and a container that is not running counts nothing.
    #[test]
    fn containers_count_what_they_use_once_reported() {
        rt().block_on(async {
            const MIB: u64 = 1 << 20;
            let fake = Arc::new(Fake::default());
            let engine = an_engine(&fake).await;
            let cell = a_box();
            engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
            let params = || StartParams {
                instance: Some(Instance::Custom { vcpu: 2.0, memory_mib: 6144, disk_mb: 16_000 }),
                ..start_img()
            };
            let run = cell.begin_run();
            engine.start(&cell, run, params()).await.expect("started");
            let cap = 6144 * MIB;
            assert_eq!(engine.reserved_memory_bytes(), cap, "no report yet");
            *fake.in_use.lock().unwrap() = vec![(cell.name.clone(), 300 * MIB), ("someone-else".into(), 1)];
            engine.refresh_in_use().await;
            assert_eq!(engine.reserved_memory_bytes(), 300 * MIB + cap / 4);
            let at = cell.state.lock().unwrap().info.in_use.expect("reported").1;
            assert_eq!(cell.counted_memory_bytes(at + IN_USE_FRESH_MS), Some(300 * MIB + cap / 4));
            assert_eq!(cell.counted_memory_bytes(at + IN_USE_FRESH_MS + 1), Some(cap), "stale: the cap");
            let run = cell.begin_run();
            engine.start(&cell, run, params()).await.expect("started again");
            assert_eq!(engine.reserved_memory_bytes(), cap, "a new run starts from its cap");
            cell.state.lock().unwrap().running = false;
            engine.refresh_in_use().await;
            assert_eq!(engine.reserved_memory_bytes(), 0, "not running");
            assert_eq!(cell.state.lock().unwrap().info.in_use, None, "a stopped container takes no report");
        });
    }

    // Goal (fork): a container is put to sleep for memory exactly as its
    // inactivity timer puts it to sleep, and that stop is the signal to
    // stop (SIGTERM), then the kill only after the grace: a guest that
    // exits at the signal is never killed (removed once it has exited), one
    // that does not is killed once the grace is out and never before. Its
    // object's monitor() sees the run end as it ended. A container its cell
    // let go of goes with its handle (a sweep); one whose loaded but idle
    // object still holds it stops the same way, its handle kept, not
    // running. Replay: a sleep is asked once (the second ask is refused).
    // Method: the fake engine records each signal and removal with when it
    // came; the grace is 400 ms here; inactivity windows of 50 ms and 60 s.
    #[test]
    fn a_sleep_for_memory_is_the_inactivity_stop_early() {
        rt().block_on(async {
            for (why, obeys, held) in [
                (Sweep::Memory, true, false),
                (Sweep::Memory, false, false),
                (Sweep::Inactivity, true, false),
                (Sweep::Inactivity, false, false),
                (Sweep::Memory, true, true),
                (Sweep::Memory, false, true),
            ] {
                let case = format!("{why:?} obeys {obeys} held {held}");
                let fake = Arc::new(Fake::default());
                fake.ends.store(true, Ordering::SeqCst);
                fake.exits_at_term.store(obeys, Ordering::SeqCst);
                let mut engine = ContainerEngine::connect(fake.clone(), "node-a".into(), None).await.unwrap();
                engine.sweep_grace = Duration::from_millis(400);
                let engine = Arc::new(engine);
                let cell = a_box();
                engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
                let run = cell.begin_run();
                engine.start(&cell, run, start_img()).await.expect("started");
                if !held {
                    cell.set_inactivity(Duration::from_millis(if why == Sweep::Memory { 60_000 } else { 50 }));
                    engine.release(&cell.scope, Release::Keep).await;
                }
                let sample = engine.samples();
                assert_eq!(sample.len(), 1);
                assert_eq!((sample[0].released_mono_ms.is_some(), sample[0].stopping), (!held, false), "{case}: {sample:?}");
                if why == Sweep::Memory {
                    assert!(engine.sleep_for_memory(&cell.scope), "{case}: it sleeps");
                    assert!(!engine.sleep_for_memory(&cell.scope), "{case}: once");
                }
                for _ in 0..500 {
                    let stopped = if held { cell.ended(run).is_some() && cell.state.lock().unwrap().sweeping.is_none() } else { engine.cell(&cell.scope).is_none() };
                    if stopped {
                        break;
                    }
                    asyncrt::sleep(Duration::from_millis(10)).await;
                }
                if held {
                    assert!(engine.cell(&cell.scope).is_some() && !cell.running(), "{case}: its cell keeps the handle, not running");
                } else {
                    assert!(engine.cell(&cell.scope).is_none(), "{case}: its handle goes once it stopped");
                }
                let asked = fake.asked.lock().unwrap().clone();
                let names: Vec<&str> = asked.iter().map(|(what, ..)| what.as_str()).collect();
                assert_eq!(names, ["start", "signal 15", "remove"], "{case}: the signal to stop, then the kill");
                let waited = asked[2].2 - asked[1].2;
                let ended = cell.ended(run).expect("the run's end is kept for its monitor()").expect("an exit");
                if obeys {
                    assert!(waited < 400, "{case}: exited at the signal, removed at once ({waited} ms)");
                    assert_eq!(ended, RunExit { code: 0, destroyed: false }, "{case}: its own exit");
                } else {
                    assert!(waited >= 400, "{case}: never killed inside the grace ({waited} ms)");
                    assert_eq!(ended, RunExit { code: 137, destroyed: true }, "{case}: the kill");
                }
            }
        });
    }

    /// A cell of class `Box` named `scope`.
    fn a_box_named(scope: &str) -> Arc<CellContainer> {
        Arc::new(CellContainer { scope: scope.into(), name: container_name("node-a", scope), ..Arc::into_inner(a_box()).unwrap() })
    }

    // Goal (fork): the engine admits a container by its whole memory, the
    // node by what its containers use, so a start can meet a full engine
    // (krun's 429) on a node that sheds nothing. Such a start makes room:
    // the idlest container that may sleep (the core's list) is put to sleep
    // as a walk down puts one (its cell's handle gone, or kept by a loaded
    // idle object), its stop ended before the start is tried once more,
    // which then runs. Invalid: with none that may sleep (the list empty,
    // or naming only the starting cell itself), the start fails as no
    // room, a typed error, and puts nothing to sleep; refused again after one slept, it fails so too,
    // having slept only that one. Each refused start reads `running` from
    // its start() (as workerd's does) until it fails, then never again,
    // and its monitor() answers the error. Method: the fake engine refuses
    // the next starts, as krun's 429; the grace is 400 ms.
    #[test]
    fn a_start_refused_for_memory_makes_room_once() {
        rt().block_on(async {
            for (case, refusals, sleepable) in [
                ("room made", 1, vec!["Box:idle"]),
                ("room made from one its idle cell holds", 1, vec!["Box:held"]),
                ("nothing may sleep", 1, vec![]),
                ("only itself", 1, vec!["Box:new"]),
                ("refused again", 2, vec!["Box:idle"]),
            ] {
                let fake = Arc::new(Fake::default());
                fake.ends.store(true, Ordering::SeqCst);
                fake.exits_at_term.store(true, Ordering::SeqCst);
                let mut engine = ContainerEngine::connect(fake.clone(), "node-a".into(), None).await.unwrap();
                engine.sweep_grace = Duration::from_millis(400);
                // the actor, as the node's answers `MakeRoom`: the core's
                // sleepable list, never the starting cell's own
                let (actor, mut asked) = mpsc::unbounded_channel();
                engine.actor = Some(actor);
                let engine = Arc::new(engine);
                let listed: Vec<String> = sleepable.iter().map(|s| s.to_string()).collect();
                let answering = engine.clone();
                asyncrt::spawn(async move {
                    while let Some(message) = asked.recv().await {
                        if let crate::actor::Message::MakeRoom { starting, reply } = message {
                            let candidates: Vec<String> = listed.iter().filter(|c| **c != starting).cloned().collect();
                            let _ = reply.send(answering.sleep_one_for_room(&candidates));
                        }
                    }
                })
                .detach();
                let [idle, held, new] = ["Box:idle", "Box:held", "Box:new"].map(a_box_named);
                for cell in [&idle, &held, &new] {
                    engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
                }
                for cell in [&idle, &held] {
                    let run = cell.begin_run();
                    engine.start(cell, run, start_img()).await.expect("started");
                }
                idle.set_inactivity(Duration::from_secs(60));
                engine.release(&idle.scope, Release::Keep).await;
                fake.full.store(refusals, Ordering::SeqCst);
                let run = new.begin_run();
                assert!(new.running(), "{case}: running from its start(), as workerd's");
                let started = engine.start(&new, run, start_img()).await;
                let slept = fake.asked_of(&idle.name);
                match case {
                    "room made from one its idle cell holds" => {
                        assert!(started.is_ok(), "{case}: {started:?}");
                        assert_eq!(fake.asked_of(&held.name), ["start", "signal 15", "remove"], "{case}: the held one slept, the signal to stop first");
                        assert_eq!(slept, ["start"], "{case}: only the one the core named");
                        assert!(engine.cell(&held.scope).is_some() && !held.running(), "{case}: its cell keeps the handle");
                        assert!(new.running(), "{case}: it runs");
                    }
                    "room made" => {
                        assert!(started.is_ok(), "{case}: {started:?}");
                        assert_eq!(slept, ["start", "signal 15", "remove"], "{case}: the idle one slept, the signal to stop first");
                        assert_eq!(fake.asked_of(&new.name), ["refused", "start"], "{case}: tried once more, after the sleep");
                        let order: Vec<String> = fake.asked.lock().unwrap().iter().map(|(what, name, _)| format!("{what} {name}")).collect();
                        let at = |x: &str| order.iter().position(|o| o == x).unwrap();
                        assert!(at(&format!("remove {}", idle.name)) < at(&format!("start {}", new.name)), "{case}: its stop ended first: {order:?}");
                        assert!(new.running() && new.ended(run).is_none(), "{case}: it runs");
                        assert_eq!(idle.ended(1), Some(Ok(RunExit { code: 0, destroyed: false })), "{case}: the slept one's monitor() sees its own exit");
                    }
                    _ => {
                        let error = started.expect_err(case);
                        let no_room = error.downcast_ref::<NoRoomForContainer>().unwrap_or_else(|| panic!("{case}: typed: {error:#}"));
                        assert_eq!(no_room.slept.is_some(), case == "refused again", "{case}: {no_room}");
                        assert!(no_room.to_string().contains("[429] the node's memory for VMs is spent"), "{case}: names the engine's error: {no_room}");
                        let tried = fake.asked_of(&new.name);
                        assert_eq!(tried, if case == "refused again" { vec!["refused", "refused"] } else { vec!["refused"] }, "{case}");
                        assert_eq!(slept, if case == "refused again" { vec!["start", "signal 15", "remove"] } else { vec!["start"] }, "{case}: at most one slept");
                        assert_eq!(fake.asked_of(&held.name), ["start"], "{case}: one the core did not name never sleeps");
                        assert!(!new.running() && new.inspect().is_none(), "{case}: refused, it never reads running again");
                        let ended = new.ended(run).expect("its monitor() is answered");
                        assert!(ended.as_ref().is_err_and(|e| e.contains("no room for the container")), "{case}: {ended:?}");
                    }
                }
            }
        });
    }

    // Goal (fork; Codex on this PR): a container whose loaded, idle object
    // holds it, put to sleep, exits at the signal; its object starts a new
    // run at once (a crash's restart) while the sleep's removal by name is
    // still under way. The new start waits for that sweep to end, so the
    // removal never reaches the new run's container, and what the sweep
    // sets after its removal is its own run's only: the new run is running.
    // Method: the fake engine holds the removal until the new start has
    // begun and is seen waiting (no second launch asked).
    #[test]
    fn a_new_run_never_meets_a_sleeps_removal() {
        rt().block_on(async {
            let fake = Arc::new(Fake::default());
            fake.ends.store(true, Ordering::SeqCst);
            fake.exits_at_term.store(true, Ordering::SeqCst);
            let mut engine = ContainerEngine::connect(fake.clone(), "node-a".into(), None).await.unwrap();
            engine.sweep_grace = Duration::from_millis(400);
            let engine = Arc::new(engine);
            let cell = a_box();
            engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
            let first = cell.begin_run();
            engine.start(&cell, first, start_img()).await.expect("started");
            fake.removal.hold();
            assert!(engine.sleep_for_memory(&cell.scope), "an idle object's container sleeps");
            fake.removal.reached().await;
            assert_eq!(cell.ended(first), Some(Ok(RunExit { code: 0, destroyed: false })), "it exited at the signal");
            // its object starts again while the removal is held
            let second = cell.begin_run();
            let starting = {
                let engine = engine.clone();
                let cell = cell.clone();
                asyncrt::spawn(async move { engine.start(&cell, second, start_img()).await })
            };
            asyncrt::sleep(Duration::from_millis(200)).await;
            assert_eq!(fake.asked_of(&cell.name), ["start", "signal 15"], "the new start waits for the sweep");
            fake.removal.open();
            starting.await.expect("the start ran").expect("started");
            assert_eq!(fake.asked_of(&cell.name), ["start", "signal 15", "remove", "start"], "removed before the new run's launch");
            assert!(cell.running() && cell.current_run() == second && cell.ended(second).is_none(), "the new run runs");
        });
    }

    // Goal (fork; Codex's second round): a start that waits past the bound
    // for a sweep whose removal is still under way never launches under
    // that name (the removal could land on it): it fails, and its object's
    // monitor() says why; once the removal ends, a start runs. Method: the
    // removal held past the bound (a grace of 400 ms and a slack of 200).
    #[test]
    fn a_start_never_launches_while_a_removal_may_land() {
        rt().block_on(async {
            let fake = Arc::new(Fake::default());
            fake.ends.store(true, Ordering::SeqCst);
            fake.exits_at_term.store(true, Ordering::SeqCst);
            let mut engine = ContainerEngine::connect(fake.clone(), "node-a".into(), None).await.unwrap();
            engine.sweep_grace = Duration::from_millis(400);
            engine.sweep_slack = Duration::from_millis(200);
            let engine = Arc::new(engine);
            let cell = a_box();
            engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
            let first = cell.begin_run();
            engine.start(&cell, first, start_img()).await.expect("started");
            fake.removal.hold();
            assert!(engine.sleep_for_memory(&cell.scope));
            fake.removal.reached().await;
            let second = cell.begin_run();
            let error = engine.start(&cell, second, start_img()).await.expect_err("past the bound, no launch");
            assert!(error.to_string().contains("still being stopped"), "{error:#}");
            assert_eq!(fake.asked_of(&cell.name), ["start", "signal 15"], "nothing launched under its name");
            assert!(!cell.running() && cell.ended(second).is_some_and(|e| e.is_err()), "its monitor() says why");
            let mut done = cell.state.lock().unwrap().sweeping.clone().expect("its sweep");
            fake.removal.open();
            while !*done.borrow_and_update() {
                done.changed().await.unwrap();
            }
            let third = cell.begin_run();
            engine.start(&cell, third, start_img()).await.expect("once the removal ended, it starts");
            assert_eq!(fake.asked_of(&cell.name), ["start", "signal 15", "remove", "start"]);
        });
    }

    // Goal (fork; Codex on this PR): a sweep whose cell took its handle back
    // before the sweep ended (an `attach` that waited past the sweep's
    // bound) leaves that handle where it is: the cell's, in the engine.
    // Method: the removal held; the handle taken back as `attach` does on
    // its timeout (released no more); then the removal let go.
    #[test]
    fn a_sweep_leaves_a_handle_its_cell_took_back() {
        rt().block_on(async {
            let fake = Arc::new(Fake::default());
            fake.ends.store(true, Ordering::SeqCst);
            fake.exits_at_term.store(true, Ordering::SeqCst);
            let mut engine = ContainerEngine::connect(fake.clone(), "node-a".into(), None).await.unwrap();
            engine.sweep_grace = Duration::from_millis(400);
            let engine = Arc::new(engine);
            let cell = a_box();
            engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
            let run = cell.begin_run();
            engine.start(&cell, run, start_img()).await.expect("started");
            cell.set_inactivity(Duration::from_secs(60));
            engine.release(&cell.scope, Release::Keep).await;
            fake.removal.hold();
            assert!(engine.sleep_for_memory(&cell.scope));
            fake.removal.reached().await;
            let done = cell.state.lock().unwrap().sweeping.clone().expect("its sweep");
            cell.state.lock().unwrap().released_mono_ms = None;
            fake.removal.open();
            let mut done = done;
            while !*done.borrow_and_update() {
                done.changed().await.unwrap();
            }
            assert!(engine.cell(&cell.scope).is_some_and(|kept| Arc::ptr_eq(&kept, &cell)), "the handle is its cell's still");
            assert!(cell.state.lock().unwrap().sweeping.is_none(), "and is swept no more: its next release arms its timer");
        });
    }

    // Goal: a report answers only for the run it was asked about. Invalid
    // (a review found it): the engine samples run A, A ends and run B
    // starts under the same name before the reply lands; B takes none of
    // A's measure and counts its cap until B itself is reported.
    #[test]
    fn an_old_runs_report_never_discounts_its_replacement() {
        rt().block_on(async {
            const MIB: u64 = 1 << 20;
            let fake = Arc::new(Fake::default());
            let engine = an_engine(&fake).await;
            let cell = a_box();
            engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
            let run = cell.begin_run();
            engine.start(&cell, run, start_img()).await.expect("started");
            *fake.in_use.lock().unwrap() = vec![(cell.name.clone(), 300 * MIB)];
            let replaced = cell.clone();
            *fake.in_use_meanwhile.lock().unwrap() = Some(Box::new(move || {
                replaced.begin_run();
            }));
            engine.refresh_in_use().await;
            assert_eq!(cell.state.lock().unwrap().info.in_use, None, "run B took run A's report");
        });
    }

    // Goal: a reply that comes back after IN_USE_FRESH_MS is never counted
    // as fresh. Invalid (a review found it): the engine samples 300 MiB,
    // the reply is delayed past the freshness window; it must not be
    // stamped at its arrival and count as new.
    #[test]
    fn a_late_report_is_never_fresh() {
        rt().block_on(async {
            const MIB: u64 = 1 << 20;
            let fake = Arc::new(Fake::default());
            let engine = an_engine(&fake).await;
            let cell = a_box();
            engine.cells.lock().unwrap().insert(cell.scope.clone(), cell.clone());
            let run = cell.begin_run();
            engine.start(&cell, run, start_img()).await.expect("started");
            *fake.in_use.lock().unwrap() = vec![(cell.name.clone(), 300 * MIB)];
            *fake.in_use_delay.lock().unwrap() = Some(Duration::from_millis(IN_USE_FRESH_MS + 100));
            engine.refresh_in_use().await;
            let fresh = cell.state.lock().unwrap().info.in_use.filter(|(_, at)| asyncrt::mono_ms().saturating_sub(*at) <= IN_USE_FRESH_MS);
            assert_eq!(fresh, None, "a report older than the window counted as fresh");
        });
    }

    // Goal (review round 2, finding 1): a rule set while the start prepares
    // its image goes out in the start's launch; when the engine refuses that
    // launch, the registration waiting on the start answers with the start's
    // error, never Ok. Forced: the start is held in its image preparation
    // until the rule is recorded, so the rule can reach the engine only in
    // the launch (checked: it was in the launch's list).
    #[test]
    fn a_rule_refused_in_its_launch_fails_its_registration() {
        rt().block_on(a_rule_refused_in_its_launch());
    }

    // Goal: the other path. A rule set once the launch has read its list
    // goes out in the start's install after it; the engine refusing that
    // install is the registration's error. Forced: the start is held in
    // its launch (its list already read) until the rule is recorded
    // (checked: the launch went out without it). And a rule set while no
    // container runs answers Ok: no start is asked to install it.
    #[test]
    fn a_rule_refused_in_the_install_after_its_launch_fails_its_registration() {
        rt().block_on(a_rule_refused_in_the_install());
    }

    async fn a_rule_refused_in_the_install() {
        let fake = Arc::new(Fake::default());
        *fake.refused.lock().unwrap() = Some("refused.example".into());
        let engine = an_engine(&fake).await;
        let cell = a_box();
        engine.intercept(&cell, http("first.example"), a_route()).await.expect("not running: recorded for the next start");
        fake.launch.hold();
        let run = cell.begin_run();
        let starting = {
            let (engine, cell) = (engine.clone(), cell.clone());
            asyncrt::spawn(async move { engine.start(&cell, run, start_img()).await })
        };
        fake.launch.reached().await;
        let registering = {
            let (engine, cell) = (engine.clone(), cell.clone());
            asyncrt::spawn(async move { engine.intercept(&cell, http("refused.example"), a_route()).await })
        };
        recorded(&cell, 2).await;
        fake.launch.open();
        starting.await.unwrap().expect("the launch itself is taken");
        let registered = registering.await.unwrap();
        assert_eq!(fake.launched.lock().unwrap().clone(), vec![http("first.example")], "the rule was not in the launch");
        let error = format!("{:#}", registered.expect_err("the install's refusal answers the registration"));
        assert!(error.contains("refused.example"), "{error}");
    }

    // Goal (review round 2, item 4: intercept_then_exec_while_starting made
    // deterministic): an exec asked while a start installs the intercepts
    // set during it (its container already started) runs only once they
    // are installed. Forced: the start is held in its launch (its list
    // read) until the rule is recorded, then held in that install while the
    // exec is asked; the fake engine records what the exec found installed.
    #[test]
    fn an_exec_during_a_start_waits_for_its_intercepts() {
        rt().block_on(an_exec_during_a_start());
    }

    async fn an_exec_during_a_start() {
        let fake = Arc::new(Fake::default());
        fake.launch.hold();
        fake.install.hold();
        let engine = an_engine(&fake).await;
        let cell = a_box();
        let run = cell.begin_run();
        let starting = {
            let (engine, cell) = (engine.clone(), cell.clone());
            asyncrt::spawn(async move { engine.start(&cell, run, start_img()).await })
        };
        fake.launch.reached().await;
        let registering = {
            let (engine, cell) = (engine.clone(), cell.clone());
            asyncrt::spawn(async move { engine.intercept(&cell, http("race.example"), a_route()).await })
        };
        recorded(&cell, 1).await;
        fake.launch.open();
        fake.install.reached().await;
        assert!(cell.state.lock().unwrap().info.started, "the container started; its intercepts are being installed");
        let execing = {
            let (engine, cell) = (engine.clone(), cell.clone());
            let params = ExecParams {
                cmd: vec!["wget".into(), "http://race.example/x".into()],
                env: Vec::new(),
                cwd: None,
                user: None,
                stdin: false,
                stdout: OutputMode::Pipe,
                stderr: OutputMode::Pipe,
                pty: None,
            };
            asyncrt::spawn(async move { engine.exec(&cell, params).await.map(|_| ()) })
        };
        asyncrt::sleep(Duration::from_millis(20)).await;
        assert!(fake.seen_by_exec.lock().unwrap().is_empty(), "the exec waits while the install is held");
        fake.install.open();
        starting.await.unwrap().unwrap();
        execing.await.unwrap().unwrap();
        registering.await.unwrap().unwrap();
        assert_eq!(fake.seen_by_exec.lock().unwrap().clone(), vec![vec!["race.example".to_string()]], "the exec ran with the rule installed");
    }

    async fn a_rule_refused_in_its_launch() {
        let fake = Arc::new(Fake::default());
        *fake.refused.lock().unwrap() = Some("refused.example".into());
        fake.image.hold();
        let engine = an_engine(&fake).await;
        let cell = a_box();
        let run = cell.begin_run();
        let starting = {
            let (engine, cell) = (engine.clone(), cell.clone());
            asyncrt::spawn(async move { engine.start(&cell, run, start_img()).await })
        };
        fake.image.reached().await;
        let registering = {
            let (engine, cell) = (engine.clone(), cell.clone());
            asyncrt::spawn(async move { engine.intercept(&cell, http("refused.example"), a_route()).await })
        };
        recorded(&cell, 1).await;
        fake.image.open();
        let started = starting.await.unwrap();
        let registered = registering.await.unwrap();
        assert_eq!(fake.launched.lock().unwrap().clone(), vec![http("refused.example")], "the rule went out in the launch");
        assert!(started.is_err(), "the engine refused the launch");
        let error = format!("{:#}", registered.expect_err("the registration answers with the start's error"));
        assert!(error.contains("refused.example"), "{error}");
    }

    fn a_cell() -> CellContainer {
        CellContainer {
            scope: "Box:1".into(),
            name: container_name("node-a", "Box:1"),
            spec: Arc::new(serde_json::from_value(json!({ "class_name": "Box" })).unwrap()),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            exit: watch::channel(None).0,
            outcomes: Mutex::new(BTreeMap::new()),
            up: watch::channel(0).0,
            intercepts_sent: tokio::sync::Mutex::new(()),
        }
    }

    fn exit(code: i64) -> Result<RunExit, String> {
        Ok(RunExit { code, destroyed: false })
    }

    // Goal (review of the stack, finding 2): a monitor() of a run keeps that
    // run's outcome through a later start, and never takes the later run's;
    // an outcome no watcher holds goes at the next start; one read goes.
    // Replay: read twice, the same outcome.
    #[test]
    fn a_run_keeps_its_own_outcome_for_its_watchers() {
        let cell = a_cell();
        let first = cell.begin_run();
        assert_eq!(cell.watch(), first);
        cell.record(first, exit(137));
        let second = cell.begin_run();
        assert_eq!(cell.ended(first), Some(exit(137)), "a later start keeps it");
        assert_eq!(cell.ended(first), Some(exit(137)), "read again, the same");
        assert_eq!(cell.ended(second), None, "the new run has not ended");
        cell.record(second, exit(0));
        assert_eq!(cell.ended(first), Some(exit(137)), "never the later run's");
        cell.unwatch(first);
        assert_eq!(cell.ended(first), None, "read by its only watcher: gone");
        // the second's outcome, watched by no one, goes at the next start
        assert_eq!(cell.ended(second), Some(exit(0)));
        let third = cell.begin_run();
        assert_eq!(cell.ended(second), None);
        // two watchers of one run: kept until both read it
        assert_eq!((cell.watch(), cell.watch()), (third, third));
        cell.record(third, Err("the engine went away".into()));
        cell.unwatch(third);
        assert!(cell.ended(third).is_some());
        cell.unwatch(third);
        assert!(cell.ended(third).is_none());
    }

    // Goal (review round 2, finding 2): an outcome a live watcher still
    // waits for is never pruned, however many there are: 70 runs, each
    // watched and ended, all still readable after the next start.
    #[test]
    fn live_watched_outcomes_survive_past_64() {
        let cell = a_cell();
        let runs: Vec<u64> = (0..70)
            .map(|_| {
                let run = cell.begin_run();
                assert_eq!(cell.watch(), run);
                cell.record(run, exit(3));
                run
            })
            .collect();
        cell.begin_run();
        for run in runs {
            assert_eq!(cell.ended(run), Some(exit(3)), "run {run}'s live watcher still reads it");
        }
    }

    // Goal (review round 2, finding 2): the outcomes only an earlier
    // instance's watchers waited for (gone with it) go once a new instance
    // makes its container; a run still running stays, and the new
    // instance's watchers keep theirs.
    #[test]
    fn abandoned_outcomes_go_when_a_new_instance_comes() {
        let cell = a_cell();
        let abandoned: Vec<u64> = (0..70)
            .map(|_| {
                let run = cell.begin_run();
                cell.watch();
                cell.record(run, exit(3));
                run
            })
            .collect();
        let running = cell.begin_run();
        cell.watch();
        cell.activate();
        for run in &abandoned {
            assert_eq!(cell.ended(*run), None, "run {run}: its watcher's instance went");
        }
        assert!(cell.outcomes.lock().unwrap().contains_key(&running), "a run still running stays");
        // the new instance watches it: its end is kept for it
        assert_eq!(cell.watch(), running);
        cell.record(running, exit(0));
        cell.begin_run();
        assert_eq!(cell.ended(running), Some(exit(0)));
        assert_eq!(cell.outcomes.lock().unwrap().len(), 1, "that run's alone: the new one is unwatched");
    }

    const QUICK: WakeTries = WakeTries {
        first: Duration::from_millis(1),
        max: Duration::from_millis(2),
        each: Duration::from_millis(50),
    };

    // Goal (review round 2, finding 3): a wake refused many times (50, past
    // the 30 tries the loop once gave up at), one of them never answering
    // (cut at its bound, then tried again), goes through once the object
    // takes it, while its watcher lives.
    #[test]
    fn a_wake_goes_through_after_many_refused_tries() {
        rt().block_on(async {
            let tries = AtomicU64::new(0);
            let wake = || {
                let n = tries.fetch_add(1, Ordering::SeqCst) + 1;
                async move {
                    if n == 3 {
                        return std::future::pending::<Wake>().await;
                    }
                    if n <= 50 {
                        Wake::Refused(format!("try {n}"))
                    } else {
                        Wake::Acknowledged
                    }
                }
            };
            assert_eq!(deliver_wake(|| true, wake, QUICK).await, (Delivered::Acknowledged, 51));
        });
    }

    // Goal: tries stop once no live watcher of the run is left (here after
    // 5), or at once when the object is gone; never acknowledged, either.
    #[test]
    fn a_wake_stops_once_no_live_watcher_is_left() {
        rt().block_on(async {
            let tries = AtomicU64::new(0);
            let awaited = || tries.load(Ordering::SeqCst) < 5;
            let wake = || {
                tries.fetch_add(1, Ordering::SeqCst);
                async { Wake::Refused("refused".into()) }
            };
            assert_eq!(deliver_wake(awaited, wake, QUICK).await, (Delivered::NoWatcherLeft, 5));
            assert_eq!(deliver_wake(|| true, || async { Wake::Gone }, QUICK).await, (Delivered::Gone, 1));
        });
    }

    #[test]
    fn names_carry_their_node() {
        let a = container_name("node-a", "Box:1");
        assert_eq!(a.len(), 30);
        assert!(a.starts_with(&node_prefix("node-a")));
        assert!(!a.starts_with(&node_prefix("node-b")));
        assert_ne!(a, container_name("node-a", "Box:2"));
    }

    #[test]
    fn intercept_entries_count_as_cloudflare_counts() {
        let rule = |target: &str| InterceptRule { scheme: "http", target: target.into() };
        assert_eq!(rule("api.example.com").entries(), 2);
        assert_eq!(rule("*.example.com").entries(), 2);
        assert_eq!(rule("*").entries(), 2);
        assert_eq!(rule("api.example.com:8443").entries(), 2);
        assert_eq!(rule("15.0.0.1:80").entries(), 1);
        assert_eq!(rule("203.0.113.0/24").entries(), 1);
    }

    #[test]
    fn engine_choice() {
        assert_eq!(EngineChoice::parse(None).unwrap(), EngineChoice::Docker);
        assert_eq!(EngineChoice::parse(Some("docker")).unwrap(), EngineChoice::Docker);
        assert_eq!(
            EngineChoice::parse(Some("krun:/var/lib/e/engine.sock")).unwrap(),
            EngineChoice::Krun("/var/lib/e/engine.sock".into())
        );
        assert_eq!(EngineChoice::parse(Some("none")).unwrap(), EngineChoice::None);
        assert!(EngineChoice::parse(Some("krun:relative.sock")).is_err());
        assert!(EngineChoice::parse(Some("podman")).is_err());
        assert!(EngineChoice::parse(Some("None")).is_err(), "the spelling is exact");
    }

    #[test]
    fn instances_to_the_engine() {
        assert_eq!(Instance::Named("dev".into()).to_json(), json!("lite"));
        assert_eq!(Instance::Named("standard".into()).to_json(), json!("standard-1"));
        assert_eq!(
            Instance::Custom { vcpu: 2.0, memory_mib: 6144, disk_mb: 8000 }.to_json(),
            json!({"vcpu": 2.0, "memoryMib": 6144, "diskMb": 8000})
        );
        assert_eq!(Instance::Named("basic".into()).resources(), Some((0.25, 1 << 30)));
    }
}
