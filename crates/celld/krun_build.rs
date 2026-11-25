// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! fork: the images of a dev node on the krun engine, built with BuildKit
//! in a microVM of that engine, so the machine needs no Docker.
//!
//! A session (`Builder`) is one `celld dev` deploy's builds. Its first build
//! starts a VM from BuildKit's own image (`BUILDKIT_IMAGE`, pinned by
//! digest) with BuildKit's state on a data disk of the engine's
//! (`CACHE_DISK`), so the cache outlives the VM; the last one's end
//! (`finish`) collects the cache below its limit, trims the disk and
//! destroys the VM, and only then has the engine load the images it
//! built: each load runs a VM of the engine's own, which an engine with
//! room for the builder alone would refuse beside it. Everything goes
//! through the engine's API: `start`, `exec` (the context in as a tar on
//! its stdin, an image out on its stdout to a file of this machine's),
//! `images/load`.
//!
//! Each image is built twice, the second time only when the engine lacks
//! it. The first build exports nothing but its manifest (`type=image`,
//! stored nowhere), whose config digest names the image: built with
//! `SOURCE_DATE_EPOCH=0`, the config carries no build time, so the same
//! layers make the same digest, and a warm boot writes no tar at all. The
//! image is then `celld-image:<config digest>`, Docker's image ID. When the
//! engine lacks that name, the build runs again, all of it cached, as a
//! `docker save` tar on the VM's disk; the tar's own config digest is
//! checked against the name before the engine loads it, so nothing is
//! ever held under a name another image's content answers to.
//!
//! What the engine gives and how it is used:
//! - its guest mounts no cgroup filesystem, which BuildKit's runc needs, so
//!   the VM's entrypoint mounts cgroup2 first (the guest has the caps);
//! - a data disk has one size, fixed when it is made, so the size is in
//!   its name: a new size is a new disk, and `DELETE /v1/data/<name>`
//!   resets the cache;
//! - the engine starts a second VM on a data disk another VM has mounted,
//!   so sessions take turns by a lock of this machine's (`lock_path`; the
//!   engine has one client user), held from the builder's start until its
//!   images are loaded: a session waits for another's (whose loads need
//!   the engine's memory too), and a builder or a staged image it finds
//!   when it holds the lock is one a killed `celld` left, and goes. The
//!   builder's one name (`VM`) and its owner's label (`OWNER_LABEL`) let
//!   anything else find it; fragment's devstack reaps one under the same
//!   lock.

// The engine's socket is I/O outside the execution boundary, as for
// container_krun, and the session is a deploy's, not a cell's.
#![allow(clippy::disallowed_methods)]

use crate::container_krun::{frames, read_frame};
use crate::docker::Docker;
use anyhow::{anyhow, bail, Context};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// BuildKit's own image: v0.35.1, the version Docker's builder on our
/// machines runs, by its index's digest.
pub const BUILDKIT_IMAGE: &str =
    "moby/buildkit:v0.35.1@sha256:baf6138eb8c37644800c7c913fba051e840ebb9ba13eaf92d92ef9a1712d0b4c";
/// The builder's sizes, largest first, with the memory each reserves:
/// Cloudflare's largest instance, 4 vCPU and 12 GiB, for a cold build's
/// compiles, down to what an engine with little room can hold (`instance`).
/// It lives only while a deploy builds, and is gone before an image loads.
const INSTANCES: [(&str, u64); 5] =
    [("standard-4", 12_288), ("standard-3", 8_192), ("standard-2", 6_144), ("standard-1", 4_096), ("basic", 1_024)];

/// The largest of `INSTANCES` the engine's free memory holds, by its health
/// (`memory_mib`, its VMs' reservations, of `memory_mib_max`).
fn instance(health: &Value) -> Option<&'static str> {
    let max = health.get("memory_mib_max")?.as_u64()?;
    let used = health.get("memory_mib")?.as_u64()?;
    let free = max.saturating_sub(used);
    INSTANCES.iter().find(|(_, mib)| *mib <= free).map(|(name, _)| *name)
}
/// The cache's disk, and its size. A cold build of fragment's stub and
/// Hermes images held 15 GB (2026-11-24); the disk holds three times
/// that under the collection's limit below, and a cold build's growth
/// above it.
const CACHE_GIB: u32 = 64;
const CACHE_DISK: &str = "celld-buildkit-64g";
const CACHE_PATH: &str = "/var/lib/buildkit";
/// The cache's limits for BuildKit's collection, in its MB: it keeps at
/// most `GC_MAX_MB`, keeps `GC_FREE_MB` of the disk free, and never
/// collects below `GC_RESERVED_MB`. The disk's 68.7 GB less `GC_MAX_MB`
/// is what one session may add before its `finish` collects again.
const GC_RESERVED_MB: u32 = 10_000;
const GC_FREE_MB: u32 = 16_000;
const GC_MAX_MB: u32 = 44_000;
// The limits fit the disk, and leave a cold Hermes build's growth free.
const _: () = assert!(GC_RESERVED_MB < GC_MAX_MB);
const _: () = assert!((GC_MAX_MB as u64 + 20_000) * 1_000_000 <= CACHE_GIB as u64 * (1 << 30));
/// Where a session's files are in the VM: on its root disk, gone with it.
const WORK: &str = "/build";
/// The builder's name on the engine, one at a time, and the label naming
/// its owner (`owner_of_self`).
const VM: &str = "celld-builder";
const OWNER_LABEL: &str = "celld-builder";
/// How long a session waits for another's to end (`lock_path`).
const LOCK_WAIT_MAX: Duration = Duration::from_secs(30 * 60);
const LOCK_WAIT_EVERY: Duration = Duration::from_millis(500);
/// One image's load into the engine, at most: it unpacks the image into a
/// disk of its own in a VM.
const LOAD_MAX: Duration = Duration::from_secs(30 * 60);
/// How long buildkitd has to answer once its VM is up.
const READY_MAX: Duration = Duration::from_secs(60);
/// One build, at most: a cold Hermes compiles for minutes.
const BUILD_MAX: Duration = Duration::from_secs(90 * 60);
/// How long a failed command's VM is watched for its end.
const ENDED_WAIT_MAX: Duration = Duration::from_secs(3);
/// buildkitd's stop, at most.
const STOP_MAX: Duration = Duration::from_secs(60);
/// One upload or small command, at most.
const STEP_MAX: Duration = Duration::from_secs(10 * 60);
/// Of a command's stderr, the last bytes kept for its error.
const TAIL_BYTES: usize = 64 * 1024;
/// Of a command's stdout read whole (a metadata file), at most.
const CAPTURE_BYTES_MAX: usize = 1 << 20;

/// One image to build: a Dockerfile, its context and its build arguments.
#[derive(Debug, Clone)]
pub struct Spec {
    pub dockerfile: PathBuf,
    pub context: PathBuf,
    pub vars: BTreeMap<String, String>,
    pub platform: Option<String>,
}

/// A command's stderr, as BuildKit's plain progress: its tail, and the
/// steps it ran and took from its cache.
#[derive(Default)]
struct Progress {
    tail: VecDeque<u8>,
    partial: Vec<u8>,
    steps: BTreeSet<u32>,
    cached: BTreeSet<u32>,
}

impl Progress {
    fn push(&mut self, bytes: &[u8]) {
        self.tail.extend(bytes);
        let over = self.tail.len().saturating_sub(TAIL_BYTES);
        self.tail.drain(..over);
        self.partial.extend_from_slice(bytes);
        while let Some(end) = self.partial.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.partial.drain(..=end).collect();
            self.line(String::from_utf8_lossy(&line).trim_end());
        }
        // A line past the tail is not a step line.
        if self.partial.len() > TAIL_BYTES {
            self.partial.clear();
        }
    }

    /// `#N [stage k/n] …` names a step, `#N CACHED` one from the cache
    /// (as fragment's xtask `cached_steps` reads Docker's). A `FROM` is no
    /// step that runs: BuildKit answers one it holds `DONE`, not `CACHED`.
    fn line(&mut self, line: &str) {
        let Some((id, rest)) = line.strip_prefix('#').and_then(|l| l.split_once(' ')) else {
            return;
        };
        let Ok(id) = id.parse::<u32>() else { return };
        let from = rest.split_once("] ").is_some_and(|(_, step)| step.starts_with("FROM "));
        if rest.starts_with('[') && !rest.starts_with("[internal]") && !rest.starts_with("[auth]") && !from {
            self.steps.insert(id);
        } else if rest == "CACHED" {
            self.cached.insert(id);
        }
    }

    fn cached_of_steps(&self) -> (usize, usize) {
        (self.cached.intersection(&self.steps).count(), self.steps.len())
    }

    fn tail(&self) -> String {
        let (a, b) = self.tail.as_slices();
        let mut bytes = a.to_vec();
        bytes.extend_from_slice(b);
        String::from_utf8_lossy(&bytes).trim_end().to_string()
    }
}

/// The owner of a session: this process, by its pid and its start (a pid
/// reused by another process starts at another time).
fn owner_of_self() -> String {
    let pid = std::process::id();
    format!("{pid}-{}", start_ticks(pid).unwrap_or(0))
}

/// `/proc/<pid>/stat`'s start time, field 22: after the command's
/// parenthesis, the 20th field.
fn start_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit_once(')')?.1.split_whitespace().nth(19)?.parse().ok()
}

/// The engine's name on this machine: 12 hex of the SHA-256 of its
/// socket's canonical path, so two paths to one socket name one engine.
fn engine_id(socket: &Path) -> String {
    use sha2::Digest as _;
    let canonical = std::fs::canonicalize(socket).unwrap_or_else(|_| socket.to_path_buf());
    let digest = sha2::Sha256::digest(canonical.as_os_str().as_encoded_bytes());
    format!("{digest:x}")[..12].to_string()
}

/// The lock sessions on the engine `engine` (`engine_id`) take turns by:
/// in this user's runtime directory, else the machine's temporary one
/// (the same for every process of the user, whatever its `TMPDIR`).
fn lock_path(engine: &str, uid: u32, runtime: Option<&Path>) -> PathBuf {
    let name = format!("celld-builder-{uid}-{engine}.lock");
    match runtime {
        Some(dir) => dir.join(name),
        None => Path::new("/tmp").join(name),
    }
}

/// Where a session keeps the images it built until the engine `engine`
/// loads them: this user's cache, on disk (a machine's `/tmp` may be
/// memory), one directory per engine, its files only while its lock is
/// held.
fn staging_dir(engine: &str) -> PathBuf {
    let cache = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
        .unwrap_or_else(std::env::temp_dir);
    cache.join("celld/krun-images").join(engine)
}

/// `dir`, made this user's alone (mode 700), its parents made as needed.
fn private_dir(dir: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .with_context(|| format!("create {}", dir.display()))?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).with_context(|| format!("make {} private", dir.display()))
}

/// A builder destroyed with no client but this blocking call: a session
/// dropped before its end (`Builder`'s `Drop`). Answers the engine's status.
fn destroy_blocking(socket: &Path, vm: &str) -> anyhow::Result<u16> {
    use std::io::{Read as _, Write as _};
    let mut c = std::os::unix::net::UnixStream::connect(socket).with_context(|| format!("connect {}", socket.display()))?;
    c.set_read_timeout(Some(Duration::from_secs(30)))?;
    c.set_write_timeout(Some(Duration::from_secs(30)))?;
    write!(c, "POST /v1/containers/{vm}/destroy HTTP/1.1\r\nhost: engine\r\nconnection: close\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\n{{}}")?;
    let mut head = [0u8; 64];
    let n = c.read(&mut head)?;
    let line = String::from_utf8_lossy(&head[..n]);
    line.split(' ').nth(1).and_then(|s| s.parse().ok()).context("the engine's answer has a status")
}

/// A pulled reference pinned to `digest`: its repository and the digest,
/// as Docker pins one, its tag dropped (a tag and a digest both could pass
/// the engine's reference length).
fn pinned(reference: &str, digest: &str) -> String {
    let repository = match reference.rfind(':') {
        // A colon after the last slash is the tag's; one before it, a
        // registry's port.
        Some(i) if !reference[i..].contains('/') => &reference[..i],
        _ => reference,
    };
    format!("{repository}@{digest}")
}

/// An image built and fetched, for the engine to load once the builder
/// is gone.
struct Staged {
    label: String,
    reference: String,
    file: PathBuf,
}

/// A config digest's hex, from BuildKit's metadata file.
fn config_digest(metadata: &[u8]) -> anyhow::Result<String> {
    let v: Value = serde_json::from_slice(metadata).context("BuildKit's metadata file is JSON")?;
    let digest = v
        .get("containerimage.config.digest")
        .and_then(Value::as_str)
        .context("BuildKit's metadata names no config digest")?;
    let hex = digest
        .strip_prefix("sha256:")
        .filter(|h| h.len() == 64 && h.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        .with_context(|| format!("a config digest {digest:?}"))?;
    Ok(hex.to_string())
}

/// The arguments of one `buildctl build` of `spec`, its Dockerfile in
/// `dockerfile_dir` and its context in `context_dir`.
fn buildctl_args(spec: &Spec, dockerfile_dir: &str, context_dir: &str, metadata: &str, output: &str) -> Vec<String> {
    let mut args: Vec<String> = [
        "buildctl",
        "build",
        "--frontend",
        "dockerfile.v0",
        "--progress",
        "plain",
    ]
    .map(String::from)
    .to_vec();
    args.extend(["--local".into(), format!("context={context_dir}")]);
    args.extend(["--local".into(), format!("dockerfile={dockerfile_dir}")]);
    args.extend(["--opt".into(), "filename=Dockerfile".into()]);
    if let Some(platform) = &spec.platform {
        args.extend(["--opt".into(), format!("platform={platform}")]);
    }
    // No build time in the config: the same layers name the same image.
    if !spec.vars.contains_key("SOURCE_DATE_EPOCH") {
        args.extend(["--opt".into(), "build-arg:SOURCE_DATE_EPOCH=0".into()]);
    }
    for (name, value) in &spec.vars {
        args.extend(["--opt".into(), format!("build-arg:{name}={value}")]);
    }
    args.extend(["--metadata-file".into(), metadata.into(), "--output".into(), output.into()]);
    args
}

/// The VM's entrypoint: cgroup2 mounted for runc, then buildkitd with its
/// collection's limits, its steps on the VM's own network.
fn entrypoint() -> Vec<String> {
    let daemon = format!(
        "exec buildkitd --oci-worker-net=host --oci-worker-gc=true \
         --oci-worker-gc-keepstorage={GC_RESERVED_MB},{GC_FREE_MB},{GC_MAX_MB}"
    );
    vec![
        "sh".into(),
        "-c".into(),
        format!("grep -q ' /sys/fs/cgroup ' /proc/mounts || mount -t cgroup2 cgroup2 /sys/fs/cgroup; {daemon}"),
    ]
}

/// A build session on the krun engine at its socket.
pub struct Builder {
    api: Docker,
    socket: PathBuf,
    /// The session's turn (`lock_path`), held from the builder's start
    /// until it is destroyed.
    lock: Option<std::fs::File>,
    vm: Option<String>,
    staged: Vec<Staged>,
    /// The contexts sent this session, by directory and ignore file: their
    /// directory in the VM.
    contexts: HashMap<(PathBuf, Option<PathBuf>), String>,
    builds: usize,
    held: Option<HashSet<String>>,
}

impl Builder {
    pub fn new(socket: &Path) -> Self {
        Self {
            api: Docker::new(socket),
            socket: socket.to_path_buf(),
            lock: None,
            vm: None,
            staged: Vec::new(),
            contexts: HashMap::new(),
            builds: 0,
            held: None,
        }
    }

    /// Builds `spec` and makes it present in the engine; answers its name
    /// there, `celld-image:<config digest>`.
    pub async fn build(&mut self, label: &str, spec: &Spec) -> anyhow::Result<String> {
        let t0 = Instant::now();
        let vm = self.vm().await?;
        let n = self.builds;
        self.builds += 1;
        let dir = format!("{WORK}/image-{n}");
        let context = self.send(&vm, spec, &dir).await.with_context(|| format!("send {label}'s build context to the builder"))?;
        let meta = format!("{dir}/built.json");
        let output = "type=image,name=celld-build,push=false,store=false";
        let progress = self
            .buildctl(&vm, &buildctl_args(spec, &dir, &context, &meta, output))
            .await
            .with_context(|| format!("build {label} in the krun engine's builder"))?;
        let key = config_digest(&self.capture(&vm, &["cat", &meta]).await?)?;
        let reference = crate::container::image_reference(&key);
        let (cached, steps) = progress.cached_of_steps();
        tracing::debug!(image = label, progress = %progress.tail(), "the build's progress, its tail");
        if self.held().await?.contains(&reference) {
            crate::note!("image {label} built in {:.1?} ({cached} of {steps} steps cached): {reference}, held by the engine", t0.elapsed());
            return Ok(reference);
        }
        // The same build, all of it cached, as a tar the engine loads.
        let tar = format!("{dir}/image.tar");
        let exported = format!("{dir}/exported.json");
        let output = format!("type=docker,dest={tar},name={reference}");
        self.buildctl(&vm, &buildctl_args(spec, &dir, &context, &exported, &output))
            .await
            .with_context(|| format!("export {label} from the krun engine's builder"))?;
        let again = config_digest(&self.capture(&vm, &["cat", &exported]).await?)?;
        if again != key {
            bail!("{label}'s export is not the image its build named ({again} against {key}): its Dockerfile does not build the same image twice");
        }
        // Fetched to this machine, for the engine to load once the builder
        // has let go of its memory (`finish`).
        let dir = staging_dir(&engine_id(&self.socket));
        private_dir(&dir)?;
        let file = dir.join(format!("{key}-{n}.tar"));
        let bytes = self.fetch(&vm, &tar, &file).await.with_context(|| format!("fetch {label} from the krun engine's builder"))?;
        self.capture(&vm, &["rm", "-f", &tar]).await?;
        self.held.get_or_insert_with(HashSet::new).insert(reference.clone());
        self.staged.push(Staged { label: label.to_string(), reference: reference.clone(), file });
        crate::note!(
            "image {label} built in {:.1?} ({cached} of {steps} steps cached): {reference}, {} MB, loaded once the builder is gone",
            t0.elapsed(),
            bytes / 1_000_000
        );
        Ok(reference)
    }

    /// A registry image, pulled by the engine itself, as `docker pull`
    /// pulls one: no Dockerfile runs, so neither do its `ONBUILD`
    /// triggers. Named by its digest, `<reference>@<digest>`, as a built
    /// image is by its content: the engine holds a tag under whatever it
    /// pulled last, and a deployment keeps the image it was given. Pulled
    /// on the session's turn, before its builder starts (`deploy`'s
    /// `resolve_declared` pulls first): the pull's own VM needs the engine's
    /// memory too.
    pub async fn pull(&mut self, reference: &str) -> anyhow::Result<String> {
        if self.lock.is_none() {
            self.take_turn().await?;
        }
        let pulled = self.engine_pull(reference).await?;
        let pinned = if reference.contains('@') {
            reference.to_string()
        } else {
            let digest = pulled
                .get("digest")
                .and_then(Value::as_str)
                .filter(|d| d.starts_with("sha256:"))
                .with_context(|| format!("the krun engine's pull of {reference} names no digest"))?;
            let pinned = pinned(reference, digest);
            // Held under the pinned name too: its manifest fetched again,
            // nothing built.
            self.engine_pull(&pinned).await?;
            pinned
        };
        crate::note!("image {reference} pulled by the krun engine: {pinned}");
        Ok(pinned)
    }

    async fn engine_pull(&self, reference: &str) -> anyhow::Result<Value> {
        let reply = self.api.call("POST", "/v1/images/pull", Some(json!({ "reference": reference }))).await?;
        anyhow::ensure!(
            reply.status.is_success(),
            "the krun engine's pull of {reference}: [{}] {}",
            reply.status.as_u16(),
            reply.message_or_error()
        );
        reply.json()
    }

    /// The session's end: the cache collected below its limits, its disk
    /// trimmed so the engine's file shrinks with it, buildkitd stopped,
    /// the VM destroyed; then the images it built loaded into the engine,
    /// and the session's turn given up. Always called, a build's failure
    /// or not.
    ///
    /// buildkitd is stopped (SIGTERM) and waited for before the VM goes:
    /// a VM destroyed under it (SIGKILL) lost the cache records of its
    /// session's last steps, so every boot built its `COPY`s again, as new
    /// layers under a new name (seen 2026-11-24).
    pub async fn finish(mut self) -> anyhow::Result<()> {
        let mut result = Ok(());
        // The name stays the session's until the VM is destroyed, and each
        // staged image until it is loaded: a deploy cancelled here (Ctrl-C)
        // leaves them to `Drop`.
        if let Some(vm) = self.vm.clone() {
            let prune = [
                "buildctl".to_string(),
                "prune".into(),
                format!("--keep-storage-min={GC_RESERVED_MB}"),
                format!("--keep-storage={GC_MAX_MB}"),
                format!("--free-storage={GC_FREE_MB}"),
            ];
            let tidied = async {
                self.run(&vm, &prune, None, tokio::io::sink(), STEP_MAX).await?;
                let df = self.capture(&vm, &["df", "-kP", CACHE_PATH]).await?;
                if let Some(used) = used_kib(&String::from_utf8_lossy(&df)) {
                    crate::note!("the image builder's cache: {:.1} GB of its {CACHE_GIB} GiB disk", used as f64 * 1024.0 / 1e9);
                }
                for cmd in [vec!["fstrim".to_string(), CACHE_PATH.into()], vec!["sync".into()]] {
                    let (code, _, p) = self.run(&vm, &cmd, None, tokio::io::sink(), STEP_MAX).await?;
                    anyhow::ensure!(code == 0, "{}: {}", cmd[0], p.tail());
                }
                self.stop(&vm).await
            }
            .await;
            if let Err(e) = &tidied {
                // A VM that ended mid-build has nothing to tidy.
                tracing::warn!(vm = %vm, error = %format!("{e:#}"), "the builder's cache was not collected");
            }
            // Under the session's turn, so the name is still this one's.
            result = self.destroy(&vm).await;
            if result.is_ok() {
                self.vm = None;
            }
        }
        // Still on the session's turn: a builder started beside a load
        // could take the memory the load's own VM needs. Bounded by the
        // session's images.
        while let Some(staged) = self.staged.first() {
            let (label, reference, file) = (staged.label.clone(), staged.reference.clone(), staged.file.clone());
            let t0 = Instant::now();
            let loaded = match tokio::time::timeout(LOAD_MAX, self.load(&file, &reference)).await {
                Ok(loaded) => loaded,
                Err(_) => Err(anyhow!("the engine did not load it within {LOAD_MAX:?}")),
            };
            let _ = std::fs::remove_file(&file);
            self.staged.remove(0);
            let staged = Staged { label, reference, file };
            match loaded {
                Ok(()) => crate::note!("image {} loaded into the krun engine in {:.1?}", staged.label, t0.elapsed()),
                Err(e) => {
                    if result.is_ok() {
                        result = Err(e.context(format!("load {} into the krun engine", staged.label)));
                    }
                }
            }
        }
        self.lock.take();
        result
    }

    /// buildkitd, the VM's entrypoint, told to stop and waited for.
    async fn stop(&self, vm: &str) -> anyhow::Result<()> {
        let reply = self.api.call("POST", &format!("/v1/containers/{vm}/signal"), Some(json!({ "signal": 15 }))).await?;
        anyhow::ensure!(reply.status.is_success(), "stop buildkitd: [{}] {}", reply.status.as_u16(), reply.message_or_error());
        match tokio::time::timeout(STOP_MAX, self.api.call("GET", &format!("/v1/containers/{vm}/wait"), None)).await {
            Ok(waited) => {
                waited?;
                Ok(())
            }
            Err(_) => bail!("buildkitd did not stop within {STOP_MAX:?}"),
        }
    }

    async fn destroy(&self, vm: &str) -> anyhow::Result<()> {
        let reply = self.api.call("POST", &format!("/v1/containers/{vm}/destroy"), Some(json!({}))).await?;
        anyhow::ensure!(
            reply.status.is_success() || reply.status.as_u16() == 404,
            "destroy the builder {vm}: [{}] {}",
            reply.status.as_u16(),
            reply.message_or_error()
        );
        Ok(())
    }

    /// The engine's image names, read once a session.
    async fn held(&mut self) -> anyhow::Result<&HashSet<String>> {
        if self.held.is_none() {
            let reply = self.api.call("GET", "/v1/images", None).await?;
            anyhow::ensure!(reply.status.is_success(), "list the krun engine's images: {}", reply.message_or_error());
            let names = reply
                .json()?
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|i| i.get("reference").and_then(Value::as_str).map(str::to_string))
                .collect();
            self.held = Some(names);
        }
        Ok(self.held.as_ref().expect("read above"))
    }

    /// The session's VM, started on its first use.
    async fn vm(&mut self) -> anyhow::Result<String> {
        if let Some(vm) = &self.vm {
            return Ok(vm.clone());
        }
        let vm = VM.to_string();
        self.take_turn().await?;
        // Set before the start: a start that failed may have left the VM.
        self.vm = Some(vm.clone());
        self.take_vm().await?;
        let t0 = Instant::now();
        // Bounded by READY_MAX.
        loop {
            let (code, _, p) = self.run(&vm, &["buildctl".into(), "debug".into(), "workers".into()], None, tokio::io::sink(), STEP_MAX).await?;
            if code == 0 {
                break;
            }
            if t0.elapsed() > READY_MAX {
                bail!("buildkitd in {vm} did not answer within {READY_MAX:?}: {}{}", p.tail(), self.ended(&vm).await);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        Ok(vm)
    }

    /// The session's turn on the engine (`lock_path`), waited for while
    /// another session has it, for `LOCK_WAIT_MAX`.
    async fn take_turn(&mut self) -> anyhow::Result<()> {
        // Held already (a pull took it): a second open of the lock's file
        // would wait on this session's own lock.
        if self.lock.is_some() {
            return Ok(());
        }
        // SAFETY: getuid reads the process's user and cannot fail.
        let uid = unsafe { libc::getuid() };
        let runtime = PathBuf::from(format!("/run/user/{uid}"));
        let engine = engine_id(&self.socket);
        let path = lock_path(&engine, uid, runtime.is_dir().then_some(runtime.as_path()));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .with_context(|| format!("open {}", path.display()))?;
        let t0 = Instant::now();
        let mut said = false;
        // Bounded by LOCK_WAIT_MAX.
        loop {
            match file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {}
                Err(std::fs::TryLockError::Error(e)) => return Err(e).with_context(|| format!("lock {}", path.display())),
            }
            if t0.elapsed() > LOCK_WAIT_MAX {
                bail!("another build still has the krun engine's image builder after {LOCK_WAIT_MAX:?} ({})", path.display());
            }
            if !said {
                crate::note!("waiting for another build to let go of the krun engine's image builder");
                said = true;
            }
            tokio::time::sleep(LOCK_WAIT_EVERY).await;
        }
        self.lock = Some(file);
        // Images staged by a session that was killed before its loads.
        if let Ok(left) = std::fs::read_dir(staging_dir(&engine)) {
            for entry in left.flatten() {
                tracing::warn!(file = %entry.path().display(), "removing an image a killed celld staged");
                let _ = std::fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    /// Starts the builder (`VM`) with the cache's disk, the session's turn
    /// held: a builder already there is one a killed session left, and goes.
    async fn take_vm(&self) -> anyhow::Result<()> {
        assert!(self.lock.is_some(), "the builder starts on the session's turn");
        // A builder there now is one a killed session left: it goes before
        // the engine's room is read, which counts it.
        if self.api.call("GET", &format!("/v1/containers/{VM}"), None).await?.status.is_success() {
            tracing::warn!("destroying an image builder a killed celld left");
            self.destroy(VM).await?;
        }
        let health = self.api.call("GET", "/v1/health", None).await?.json()?;
        let Some(instance) = instance(&health) else {
            bail!(
                "the krun engine has no room for the image builder: {} of its {} MiB in use, and the smallest builder takes {} MiB",
                health["memory_mib"], health["memory_mib_max"], INSTANCES[INSTANCES.len() - 1].1
            );
        };
        if instance != INSTANCES[0].0 {
            crate::note!("the image builder runs as {instance}: the krun engine has no room for {}", INSTANCES[0].0);
        }
        let start = json!({
            "image": BUILDKIT_IMAGE,
            "enableInternet": true,
            "entrypoint": entrypoint(),
            "instance": instance,
            "data": { "name": CACHE_DISK, "path": CACHE_PATH, "gib": CACHE_GIB },
            "labels": { OWNER_LABEL: owner_of_self() },
        });
        // Bounded: one leftover destroyed, then one start more.
        for attempt in 0..2 {
            let reply = self.api.call("POST", &format!("/v1/containers/{VM}/start?wait=ready"), Some(start.clone())).await?;
            if reply.status.is_success() {
                return Ok(());
            }
            let message = reply.message_or_error();
            match reply.status.as_u16() {
                409 if attempt == 0 => {
                    tracing::warn!("destroying an image builder a killed celld left");
                    self.destroy(VM).await?;
                }
                429 => bail!("the krun engine has no room for the image builder ({instance}): {message}"),
                status => bail!("start the image builder {VM}: [{status}] {message}"),
            }
        }
        unreachable!("the second attempt answers")
    }

    /// Sends `spec`'s Dockerfile (to `dir`) and, once a session, its
    /// context, as one tar into the VM; answers the context's directory.
    async fn send(&mut self, vm: &str, spec: &Spec, dir: &str) -> anyhow::Result<String> {
        let ignore_path = crate::build_context::ignore_file(&spec.dockerfile, &spec.context);
        let key = (spec.context.clone(), ignore_path.clone());
        let known = self.contexts.get(&key).cloned();
        let context_dir = known.clone().unwrap_or_else(|| format!("{WORK}/context-{}", self.contexts.len()));
        let file = tempfile::tempfile().context("a file for the build context")?;
        let mut tar = tar::Builder::new(file);
        let rel = |d: &str| d.strip_prefix(&format!("{WORK}/")).expect("below WORK").to_string();
        let dockerfile = std::fs::read(&spec.dockerfile).with_context(|| format!("read {}", spec.dockerfile.display()))?;
        append_bytes(&mut tar, &format!("{}/Dockerfile", rel(dir)), &dockerfile)?;
        let ignore_text = match &ignore_path {
            Some(p) => std::fs::read_to_string(p).with_context(|| format!("read {}", p.display()))?,
            None => String::new(),
        };
        // BuildKit filters the context again by the same file, so it goes
        // where BuildKit looks: beside the Dockerfile when it is the
        // Dockerfile's own, else at the context's root.
        let shared = ignore_path.as_ref().is_some_and(|p| *p == spec.context.join(".dockerignore"));
        if ignore_path.is_some() && !shared {
            append_bytes(&mut tar, &format!("{}/Dockerfile.dockerignore", rel(dir)), ignore_text.as_bytes())?;
        }
        let mut written = crate::build_context::Written::default();
        if known.is_none() {
            let ignore = crate::build_context::Ignore::parse(&ignore_text).with_context(|| format!("parse {:?}", ignore_path))?;
            written = crate::build_context::write(&mut tar, &spec.context, &rel(&context_dir), &ignore)?;
            if shared && ignore.matches_using_parent(".dockerignore", None).0 {
                append_bytes(&mut tar, &format!("{}/.dockerignore", rel(&context_dir)), ignore_text.as_bytes())?;
            }
        }
        let mut file = tar.into_inner().context("finish the build context's tar")?;
        use std::io::Seek as _;
        file.rewind()?;
        let body: Box<dyn AsyncRead + Send + Unpin> = Box::new(tokio::fs::File::from_std(file));
        let script = format!("mkdir -p {WORK} && tar -x -f - -C {WORK}");
        let (code, _, p) = self.run(vm, &["sh".into(), "-c".into(), script], Some(body), tokio::io::sink(), STEP_MAX).await?;
        anyhow::ensure!(code == 0, "unpack the build context (exit {code}): {}{}", p.tail(), self.ended(vm).await);
        if known.is_none() {
            tracing::info!(context = %spec.context.display(), files = written.entries, bytes = written.bytes, "build context sent to the builder");
            self.contexts.insert(key, context_dir.clone());
        }
        Ok(context_dir)
    }

    /// One `buildctl build`, failing with its output's tail, and the VM's
    /// end when it ended under the build.
    ///
    /// BuildKit's copy of a local source (`--local`) from an earlier build is
    /// dropped first: BuildKit sends only the files whose size, mode or
    /// time changed since it, and a tar keeps whole seconds, so a file
    /// rewritten at its length within its second (or another context's file
    /// at the same path) came from that copy (seen 2026-11-25). The context
    /// goes over whole each build, and a `COPY` of unchanged bytes is still
    /// cached: BuildKit keys it by the bytes.
    async fn buildctl(&self, vm: &str, args: &[String]) -> anyhow::Result<Progress> {
        let prune = ["buildctl", "prune", "--filter", "type==source.local"].map(String::from);
        let (code, _, p) = self.run(vm, &prune, None, tokio::io::sink(), STEP_MAX).await?;
        anyhow::ensure!(code == 0, "drop BuildKit's earlier context: {}{}", p.tail(), self.ended(vm).await);
        let (code, _, p) = match self.run(vm, args, None, tokio::io::sink(), BUILD_MAX).await {
            Ok(ran) => ran,
            Err(e) => bail!("{e:#}{}", self.ended(vm).await),
        };
        if code != 0 {
            bail!("buildctl exited {code}:\n{}{}", p.tail(), self.ended(vm).await);
        }
        Ok(p)
    }

    /// A small command's stdout, whole.
    async fn capture(&self, vm: &str, cmd: &[&str]) -> anyhow::Result<Vec<u8>> {
        let cmd: Vec<String> = cmd.iter().map(|s| s.to_string()).collect();
        let (code, out, p) = self.run(vm, &cmd, None, Vec::new(), STEP_MAX).await?;
        anyhow::ensure!(code == 0, "{} exited {code}: {}{}", cmd.join(" "), p.tail(), self.ended(vm).await);
        anyhow::ensure!(out.len() <= CAPTURE_BYTES_MAX, "{} wrote more than {CAPTURE_BYTES_MAX} bytes", cmd.join(" "));
        Ok(out)
    }

    /// The tar at `tar` in the VM, written to `file` on this machine;
    /// answers its bytes.
    async fn fetch(&self, vm: &str, tar: &str, file: &Path) -> anyhow::Result<u64> {
        use std::os::unix::fs::OpenOptionsExt as _;
        // The image's layers hold the project's files: this user's alone.
        let out = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(file)
            .with_context(|| format!("create {}", file.display()))?;
        let out = tokio::fs::File::from_std(out);
        let cat = ["cat".to_string(), tar.to_string()];
        let (code, out, p) = match self.run(vm, &cat, None, Counted { inner: out, bytes: 0 }, BUILD_MAX).await {
            Ok(ran) => ran,
            Err(e) => {
                let _ = std::fs::remove_file(file);
                bail!("{e:#}{}", self.ended(vm).await)
            }
        };
        if code != 0 {
            let _ = std::fs::remove_file(file);
            bail!("read {tar} (exit {code}): {}{}", p.tail(), self.ended(vm).await);
        }
        out.inner.sync_all().await.with_context(|| format!("write {}", file.display()))?;
        Ok(out.bytes)
    }

    /// The image tar at `file` loaded into the engine under `reference`.
    async fn load(&self, file: &Path, reference: &str) -> anyhow::Result<()> {
        let body = tokio::fs::File::open(file).await.with_context(|| format!("open {}", file.display()))?;
        let path = format!(
            "/v1/images/load?reference={}",
            percent_encoding::utf8_percent_encode(reference, percent_encoding::NON_ALPHANUMERIC)
        );
        self.api.post_stream(&path, body, "load image").await.map(drop)
    }

    /// Why a command in the VM may have failed: the VM itself ended, with
    /// buildkitd's last words while the engine keeps them; empty while it
    /// runs. A command killed as its VM went down answers before the engine
    /// says the VM ended, so a VM still running is asked again, for
    /// `ENDED_WAIT_MAX`; one the engine no longer has ended with its words.
    async fn ended(&self, vm: &str) -> String {
        let t0 = Instant::now();
        // Bounded by ENDED_WAIT_MAX.
        let v = loop {
            let Ok(reply) = self.api.call("GET", &format!("/v1/containers/{vm}"), None).await else {
                return String::new();
            };
            if reply.status.as_u16() == 404 {
                return format!("\nthe builder VM {vm} ended under it: its entrypoint, buildkitd, exited, and the engine let the VM go");
            }
            let Ok(v) = reply.json() else { return String::new() };
            if v.get("running").and_then(Value::as_bool) == Some(false) {
                break v;
            }
            if t0.elapsed() > ENDED_WAIT_MAX {
                return String::new();
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let logs = match self.api.call("GET", &format!("/v1/containers/{vm}/logs"), None).await {
            Ok(r) => r.json().ok().and_then(|l| l.get("stderr")?.as_str().map(str::to_string)).unwrap_or_default(),
            Err(_) => String::new(),
        };
        let last: Vec<&str> = logs.lines().rev().take(8).collect::<Vec<_>>().into_iter().rev().collect();
        format!("\nthe builder VM {vm} ended ({}); buildkitd's last lines:\n{}", v.get("exit").unwrap_or(&Value::Null), last.join("\n"))
    }

    /// One process in the VM: `stdin` sent to it, its stdout written to
    /// `out` (given back), its stderr kept as progress; answers its exit
    /// code. Bounded by `max`.
    async fn run<W: AsyncWrite + Unpin + Send>(
        &self,
        vm: &str,
        cmd: &[String],
        stdin: Option<Box<dyn AsyncRead + Send + Unpin>>,
        out: W,
        max: Duration,
    ) -> anyhow::Result<(i64, W, Progress)> {
        let body = json!({ "cmd": cmd, "stdin": stdin.is_some(), "stdout": "pipe", "stderr": "pipe" });
        let what = cmd.first().map(String::as_str).unwrap_or_default().to_string();
        let ran = tokio::time::timeout(max, async {
            let stream = self.api.upgrade(&format!("/v1/containers/{vm}/exec"), body, "sandcastle-exec").await?;
            let (mut read, mut write) = tokio::io::split(stream);
            let feed = async {
                let Some(mut stdin) = stdin else { return anyhow::Ok(()) };
                let mut buf = vec![0u8; frames::PAYLOAD_BYTES_MAX];
                // Bounded by the input's length.
                loop {
                    let n = stdin.read(&mut buf).await.context("read the input")?;
                    write.write_all(&frames::encode(frames::STDIN, &buf[..n])).await.context("exec stream")?;
                    if n == 0 {
                        return write.flush().await.context("exec stream");
                    }
                }
            };
            let pump = async {
                let mut out = out;
                let mut progress = Progress::default();
                // Bounded by the engine's stream, which ends with the exit.
                loop {
                    let Some((stream, payload)) = read_frame(&mut read).await? else {
                        bail!("the exec stream ended before {what} exited");
                    };
                    match stream {
                        frames::STARTED => {}
                        frames::STDOUT => out.write_all(&payload).await.context("write the output")?,
                        frames::STDERR => progress.push(&payload),
                        frames::EXITED => {
                            let v: Value = serde_json::from_slice(&payload).unwrap_or(Value::Null);
                            let code = match (v.get("code").and_then(Value::as_i64), v.get("signal").and_then(Value::as_i64)) {
                                (Some(code), _) => code,
                                (None, Some(signal)) => 128 + signal,
                                _ => bail!("{what} exited without a code"),
                            };
                            out.shutdown().await.context("close the output")?;
                            return Ok((code, out, progress));
                        }
                        frames::ERROR => bail!("exec {what}: {}", String::from_utf8_lossy(&payload)),
                        other => bail!("exec {what}: frame {other} from the engine"),
                    }
                }
            };
            let (feed, pump) = (std::pin::pin!(feed), std::pin::pin!(pump));
            match futures_util::future::select(feed, pump).await {
                futures_util::future::Either::Left((fed, pump)) => {
                    let pumped = pump.await?;
                    // An input the process stopped reading fails only if it
                    // exited 0.
                    if pumped.0 == 0 {
                        fed?;
                    }
                    Ok(pumped)
                }
                // Exited before it read all of its input, which no longer
                // matters: the stream is not waited on.
                futures_util::future::Either::Right((pumped, _)) => pumped,
            }
        })
        .await;
        match ran {
            Ok(result) => result,
            Err(_) => Err(anyhow!("{what} in the builder did not finish within {max:?}")),
        }
    }
}

/// The KiB used, from `df -kP`'s line for one filesystem.
fn used_kib(df: &str) -> Option<u64> {
    df.lines().nth(1)?.split_whitespace().nth(2)?.parse().ok()
}

/// A session dropped before its end, as `celld dev`'s Ctrl-C drops a
/// deploy mid-build: its builder is destroyed at once, still on the
/// session's turn, and its staged images go; the turn goes with the lock's
/// file. `finish` leaves nothing for this to do.
impl Drop for Builder {
    fn drop(&mut self) {
        if let Some(vm) = self.vm.take() {
            match destroy_blocking(&self.socket, &vm) {
                Ok(200 | 404) => {}
                Ok(status) => tracing::warn!(vm = %vm, status, "the builder was not destroyed"),
                Err(e) => tracing::warn!(vm = %vm, error = %format!("{e:#}"), "the builder was not destroyed"),
            }
        }
        for staged in self.staged.drain(..) {
            let _ = std::fs::remove_file(&staged.file);
        }
    }
}

/// A file of `bytes` at `path` in `tar`, mode 644.
fn append_bytes<W: std::io::Write>(tar: &mut tar::Builder<W>, path: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, path, bytes).with_context(|| format!("add {path}"))
}

/// A writer that counts what passes.
struct Counted<W> {
    inner: W,
    bytes: u64,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Counted<W> {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let polled = std::pin::Pin::new(&mut self.inner).poll_write(cx, buf);
        if let std::task::Poll::Ready(Ok(n)) = &polled {
            self.bytes += *n as u64;
        }
        polled
    }

    fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Goal: BuildKit's plain progress is read as xtask reads Docker's.
    #[test]
    fn progress_counts_steps_and_hits_across_chunks() {
        let mut p = Progress::default();
        p.push(b"#1 [internal] load build definition\n#5 [build 1/3] FROM rust\n#5 DONE 0.1s\n#8 [build 2/3] COPY a b\n#8 CA");
        p.push(b"CHED\n#6 [build 3/3] RUN cargo build\n#6 0.3 Compiling\n#7 [stage-0 1/1] COPY x\n#7 CACHED\n");
        assert_eq!(p.cached_of_steps(), (2, 3), "the FROM is not counted");
        assert!(p.tail().ends_with("#7 CACHED"));
    }

    // Goal: the builder takes the largest size the engine has room for.
    #[test]
    fn the_builder_fits_the_engines_room() {
        let h = |used: u64, max: u64| json!({ "memory_mib": used, "memory_mib_max": max });
        assert_eq!(instance(&h(1024, 32_768)), Some("standard-4"));
        assert_eq!(instance(&h(0, 7_168)), Some("standard-2"), "Sandcastle's default budget");
        assert_eq!(instance(&h(0, 8_192)), Some("standard-3"));
        assert_eq!(instance(&h(0, 5_120)), Some("standard-1"));
        assert_eq!(instance(&h(6_000, 7_168)), Some("basic"));
        assert_eq!(instance(&h(7_000, 7_168)), None);
        assert_eq!(instance(&json!({})), None);
    }

    // Goal: a pull is pinned as Docker pins one, its tag dropped.
    #[test]
    fn a_pull_is_pinned_by_its_digest() {
        let d = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(pinned("redis:7", d), format!("redis@{d}"));
        assert_eq!(pinned("redis", d), format!("redis@{d}"));
        assert_eq!(pinned("localhost:5000/a/b:v1", d), format!("localhost:5000/a/b@{d}"));
        assert_eq!(pinned("localhost:5000/a/b", d), format!("localhost:5000/a/b@{d}"));
    }

    #[test]
    fn the_cache_reads_its_use_from_df() {
        let df = "Filesystem           1K-blocks      Used Available Use% Mounted on\n/dev/vdd              65478188  15834112  49644076  24% /var/lib/buildkit\n";
        assert_eq!(used_kib(df), Some(15_834_112));
        assert_eq!(used_kib("Filesystem\n"), None);
    }

    #[test]
    fn the_tail_is_bounded() {
        let mut p = Progress::default();
        for _ in 0..100 {
            p.push(&[b'x'; 4096]);
        }
        assert_eq!(p.tail.len(), TAIL_BYTES);
    }

    // Goal: the name is the config digest, and nothing else passes for one.
    #[test]
    fn the_metadata_names_the_config() {
        let hex = "f94f07602930371ab226f35d2acafbd61b0b0c073c3fd88c6ec148a680d6c3d7";
        let meta = format!(r#"{{"containerimage.config.digest":"sha256:{hex}","containerimage.digest":"sha256:00"}}"#);
        assert_eq!(config_digest(meta.as_bytes()).unwrap(), hex);
        assert!(config_digest(br#"{"containerimage.config.digest":"sha256:F94F"}"#).is_err());
        assert!(config_digest(b"{}").is_err());
        assert!(config_digest(b"not json").is_err());
    }

    #[test]
    fn a_build_pins_its_time_and_passes_its_vars() {
        let spec = Spec {
            dockerfile: "/x/Dockerfile".into(),
            context: "/x".into(),
            vars: BTreeMap::from([("V".to_string(), "2 words".to_string())]),
            platform: Some("linux/amd64".into()),
        };
        let args = buildctl_args(&spec, "/build/image-0", "/build/context-0", "/build/image-0/built.json", "type=image");
        let joined = args.join(" ");
        assert!(joined.contains("--opt build-arg:SOURCE_DATE_EPOCH=0"));
        assert!(args.contains(&"build-arg:V=2 words".to_string()), "a value is one argument");
        assert!(joined.contains("--opt platform=linux/amd64"));
        assert!(joined.contains("--local context=/build/context-0 --local dockerfile=/build/image-0"));
        let mut own = spec.clone();
        own.vars.insert("SOURCE_DATE_EPOCH".into(), "7".into());
        let args = buildctl_args(&own, "d", "c", "m", "o");
        assert!(!args.contains(&"build-arg:SOURCE_DATE_EPOCH=0".to_string()));
        assert!(args.contains(&"build-arg:SOURCE_DATE_EPOCH=7".to_string()));
    }

    // Goal: the builder names its owner as a label may hold it: its pid
    // and when that process started.
    #[test]
    fn a_builder_names_its_owner() {
        let mine = owner_of_self();
        assert!(mine.len() <= 64, "a label's value");
        assert!(OWNER_LABEL.len() <= 16, "a label's name");
        let (pid, start) = mine.split_once('-').unwrap();
        assert_eq!(pid.parse::<u32>().unwrap(), std::process::id());
        assert!(start.parse::<u64>().unwrap() > 0, "read from /proc");
    }

    // Goal: every session of the user on one engine takes its turn by one
    // lock, whatever its environment; another engine has its own.
    #[test]
    fn sessions_on_one_engine_share_a_lock() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("engine.sock");
        std::fs::write(&socket, "").unwrap();
        std::os::unix::fs::symlink(&socket, dir.path().join("link.sock")).unwrap();
        assert_eq!(engine_id(&socket), engine_id(&dir.path().join("link.sock")), "two paths to one socket");
        assert_ne!(engine_id(&socket), engine_id(Path::new("/srv/other/engine.sock")));
        let run = Path::new("/run/user/1000");
        let a = lock_path(&engine_id(&socket), 1000, Some(run));
        assert!(a.starts_with(run) && a.to_string_lossy().ends_with(&format!("-{}.lock", engine_id(&socket))));
        assert!(lock_path("e", 1000, None).starts_with("/tmp"));
        assert_ne!(staging_dir("a"), staging_dir("b"), "each engine's images apart");
        let path = dir.path().join("l.lock");
        let first = std::fs::File::create(&path).unwrap();
        first.try_lock().unwrap();
        let second = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        assert!(matches!(second.try_lock(), Err(std::fs::TryLockError::WouldBlock)), "a second session waits");
        drop(first);
        second.try_lock().unwrap();
    }

    #[test]
    fn the_entrypoint_mounts_cgroups_then_runs_buildkitd_with_its_limits() {
        let e = entrypoint();
        assert_eq!(&e[..2], ["sh", "-c"]);
        assert!(e[2].contains("mount -t cgroup2 cgroup2 /sys/fs/cgroup"));
        assert!(e[2].contains(&format!("--oci-worker-gc-keepstorage={GC_RESERVED_MB},{GC_FREE_MB},{GC_MAX_MB}")));
        assert!(CACHE_DISK.ends_with(&format!("-{CACHE_GIB}g")), "a new size is a new disk");
    }
}
