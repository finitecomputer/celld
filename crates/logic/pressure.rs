// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Memory-pressure shedding as a pure classifier of a memory sample plus the
//! prior shedding state (the hysteresis latch). No I/O, no clock — the env
//! read that builds the config stays at the edge (`main.rs`), and so does the
//! measurement, which has to ask both the operating system and the allocator.
//!
//! Residency is deliberately *not* here. A node's cell count is a hard cap
//! enforced at admission ([`crate::State::has_capacity`]), self-limiting and
//! known exactly; it is not a resource that needs a proactive walk down. This
//! classifier answers only the other question — "is this node out of memory
//! and must give cells back to recover?" — which a cell count cannot answer.
//! Conflating the two produced the placement churn and the admission wedge;
//! splitting them is what keeps each decision small.

/// Resource watermarks. Built once from the environment by the caller; the
/// core never reads the environment itself. Residency has no watermark here —
/// it is capped at admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PressureConfig {
    /// The ceiling on the memory the cells hold. This is the ordinary limit,
    /// and `CELLD_MAX_RSS_MB` sets it.
    pub high_bytes: Option<u64>,
    /// An absolute ceiling on the cgroup charge, or process RSS when the
    /// process has no readable cgroup: 95% of the available memory.
    ///
    /// `high_bytes` applies to allocator-adjusted RSS and to the active cgroup
    /// working set after the same allocator adjustment. The adjustment lets a
    /// node recover after a free, and the cgroup measurement includes active
    /// kernel charges outside RSS. Neither measurement includes every charge
    /// that the cgroup limit uses.
    ///
    /// This is the floor under that trade. A container reads
    /// `memory.current`, so kernel charges outside process RSS cannot hide
    /// from it.
    ///
    /// It is a fixed share of the machine and is never derived from
    /// `high_bytes`. A first attempt placed it "at least 125% of the ceiling",
    /// which put it at exactly 100% of the machine for the default ceiling of
    /// 80% -- above anything the kernel would let the process reach, so the
    /// floor did not exist in the configuration that ships. See
    /// [`PressureConfig::from_limits`].
    pub rss_hard_bytes: Option<u64>,
}

/// Which ceilings are currently latched. Each one engages at its own ceiling
/// and releases at its own low watermark, so one crossing can never hold the
/// node against the other's watermark.
///
/// This is two booleans rather than the reported reason, because the reason
/// names only the more serious crossing. Deriving the latches from it lets a
/// hard-cap crossing arm the ordinary ceiling, and then a node stays closed on
/// a ceiling it never crossed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Latches {
    pub memory: bool,
    pub rss_hard: bool,
}

/// Which measurement a walk down is working against.
///
/// The futility stop compares a sample with the one the last cut measured, so
/// it has to know that the two samples are the same measurement. They are not
/// interchangeable: eviction moves the ordinary working-set figure at once and
/// may never move the complete cgroup charge at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metric {
    /// The greater of allocator-adjusted RSS and the allocator-adjusted active
    /// cgroup working set. An eviction can return this memory.
    InUse,
    /// The complete cgroup charge, with process RSS as a host fallback.
    Rss,
}

/// A resource sample — the only input the classifier reads. `resident_cells`
/// is carried so a resource trigger can size its walk down as a proportion of
/// what is actually resident.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Load {
    pub resident_cells: usize,
    /// Reported to peers and operators, never decided on.
    pub rss_bytes: u64,
    /// RSS minus the pages the allocator keeps but nothing uses. This is the
    /// ordinary pressure fallback when no cgroup working set is readable.
    pub in_use_bytes: u64,
    /// The cgroup charge after inactive file pages from `memory.stat` are
    /// removed. `None` outside a readable Linux memory cgroup.
    pub cgroup_working_set_bytes: Option<u64>,
    /// The complete `memory.current` charge. This is the number the cgroup
    /// limit constrains, including charges that process RSS does not contain.
    /// `None` outside a readable Linux memory cgroup.
    pub cgroup_current_bytes: Option<u64>,
    /// Memory the node commits to its containers, outside this process.
    ///
    /// A container runs in its own cgroup, a sibling of the node's, so its
    /// memory is in neither the process RSS nor the node's cgroup charge. It
    /// is real memory on the machine all the same. Each running container
    /// counts as [`container_counted_bytes`] says: its instance-type cap
    /// until its engine reports what it uses, then that plus room to grow.
    /// Both measurements add it: the ordinary ceiling so the node stops
    /// taking cells before the machine fills, and the hard cap so the
    /// absolute limit sees the whole charge.
    pub container_reserved_bytes: u64,
}

impl Load {
    /// The ordinary pressure measurement. A cgroup working set contains active
    /// kernel charges that process RSS omits. Subtract the same measured
    /// allocator slack from it, or retained pages would recreate the admission
    /// wedge through the cgroup path. The process measurement remains the floor
    /// because cgroup and RSS accounting can differ for shared pages.
    pub fn memory_bytes(self) -> u64 {
        let allocator_slack = self.rss_bytes.saturating_sub(self.in_use_bytes);
        let cgroup_in_use = self
            .cgroup_working_set_bytes
            .unwrap_or_default()
            .saturating_sub(allocator_slack);
        self.in_use_bytes
            .max(cgroup_in_use)
            .saturating_add(self.container_reserved_bytes)
    }

    /// The hard pressure measurement. `memory.current` is authoritative when
    /// present because the cgroup limit constrains it, not process RSS.
    pub fn hard_bytes(self) -> u64 {
        self.cgroup_current_bytes
            .unwrap_or(self.rss_bytes)
            .saturating_add(self.container_reserved_bytes)
    }

    pub fn metric_bytes(self, metric: Metric) -> u64 {
        match metric {
            Metric::InUse => self.memory_bytes(),
            Metric::Rss => self.hard_bytes(),
        }
    }
}

/// The share of a container's cap counted on top of what it uses, as room to
/// grow between samples: a quarter.
pub const CONTAINER_HEADROOM_DIVISOR: u64 = 4;

/// What one running container counts against the node's memory.
///
/// `cap` is its instance type's memory, which its engine enforces as the
/// container's hard limit. `in_use` is what the engine last reported the
/// container holding that the machine cannot take back (a krun VM's charge
/// less the host's page cache of its disks), or `None` when there is no
/// fresh report: an engine that reports none (Docker), a container still
/// starting, or an engine that stopped answering.
///
/// Without a report the cap counts, as it always did: a container can grow
/// to it at any moment. With one, an idle computer holding 300 MiB of a
/// 6 GiB cap counts 300 MiB plus a quarter of its cap, so a node admits by
/// what its containers use and not by what they may use; counting caps
/// turned a 12 GiB box away from its second computer. The quarter is room
/// to grow between reports. The count never passes the cap, which is all
/// the container can ever hold.
pub fn container_counted_bytes(cap: u64, in_use: Option<u64>) -> u64 {
    match in_use {
        None => cap,
        Some(bytes) => bytes
            .saturating_add(cap / CONTAINER_HEADROOM_DIVISOR)
            .min(cap),
    }
}

/// One running container, as the shell samples it for the walk down.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerSample {
    /// The cell (the Durable Object) the container belongs to.
    pub cell: String,
    /// What it counts against the node now: [`container_counted_bytes`].
    pub counted_bytes: u64,
    /// When its cell left this node's runtime and its container stayed on
    /// its inactivity timer (monotonic ms), or `None` while the cell holds
    /// it. Only a container its cell has let go of is ever put to sleep.
    pub released_mono_ms: Option<u64>,
    /// A stop of it is under way: what it counts is already on its way
    /// back.
    pub stopping: bool,
}

/// A container the walk down may put to sleep, weighed with what the core
/// knows of its cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SleepCandidate<'a> {
    pub cell: &'a str,
    pub counted_bytes: u64,
    /// Its object is at work, or about to be: an open socket (a hibernated
    /// one too), a request in flight or waiting for it, an alarm firing, or
    /// its cell between phases. Such a container is never put to sleep. An
    /// object loaded but doing none of that is not resident here.
    pub resident: bool,
    /// The cell's last activity (monotonic ms).
    pub last_active_mono_ms: u64,
    pub stopping: bool,
}

/// How long a container's cell must have done nothing before a walk down
/// may put the container to sleep: two minutes.
///
/// An object may still save what its container did after its last activity:
/// Fragment's computer saves `/data` 30 s after its guest's last turn or its
/// owner's last port tab ends, then holds the guest and copies. A stop
/// inside that delay cuts the save short and loses that work, so the window
/// exceeds it, with the save's own time on top.
pub const CONTAINER_QUIET_MS: u64 = 2 * 60_000;

/// The containers a latched walk down puts to sleep, in the order it puts
/// them: none while the evictions are projected to bring `sample_bytes` to
/// `resume_line` (`evictions_return_bytes`); otherwise the least recently
/// active first, and only as many as the rest needs, counting a stop
/// already under way as memory on its way back. Never one whose object is
/// resident, nor one whose cell did anything in the last
/// [`CONTAINER_QUIET_MS`]: what can't be put to sleep stays, and the latch
/// holds.
///
/// A sleep is the stop the container's inactivity timer makes, early: its
/// object sees the same end, and its next activation starts it again.
pub fn containers_to_sleep<'a>(
    sample_bytes: u64,
    resume_line: u64,
    evictions_return_bytes: u64,
    candidates: &[SleepCandidate<'a>],
    now_mono_ms: u64,
) -> Vec<&'a str> {
    let stopping: u64 = candidates
        .iter()
        .filter(|c| c.stopping)
        .map(|c| c.counted_bytes)
        .fold(0, u64::saturating_add);
    let mut need = sample_bytes
        .saturating_sub(resume_line)
        .saturating_sub(evictions_return_bytes)
        .saturating_sub(stopping);
    let mut chosen = Vec::new();
    for candidate in sleepable(candidates, now_mono_ms) {
        if need == 0 {
            break;
        }
        chosen.push(candidate.cell);
        need = need.saturating_sub(candidate.counted_bytes);
    }
    chosen
}

/// The containers that may be put to sleep now, the least recently active
/// first: none whose object is resident, none already stopping, none whose
/// cell did anything in the last [`CONTAINER_QUIET_MS`]. The walk down takes
/// as many of them as it needs ([`containers_to_sleep`]); a start the
/// engine refuses for want of memory takes the first.
pub fn sleepable<'a, 'b>(candidates: &'b [SleepCandidate<'a>], now_mono_ms: u64) -> Vec<&'b SleepCandidate<'a>> {
    let mut idle: Vec<&SleepCandidate<'a>> = candidates
        .iter()
        .filter(|c| {
            !c.resident
                && !c.stopping
                && now_mono_ms.saturating_sub(c.last_active_mono_ms) >= CONTAINER_QUIET_MS
        })
        .collect();
    // Least recently active first; the cell breaks a tie, so the choice is
    // a function of the sample and not of its order.
    idle.sort_by_key(|c| (c.last_active_mono_ms, c.cell));
    idle
}

/// The latch engaged because the memory the cells hold crossed the ceiling.
/// This is the ordinary case, and shedding relieves it.
pub const SHED_MEMORY: &str = "memory";

/// The latch engaged because the complete cgroup charge, or the RSS fallback,
/// crossed the absolute cap. The string keeps its established telemetry name,
/// but a Linux container decides it from `memory.current`.
pub const SHED_RSS_HARD: &str = "rss-hard";

impl PressureConfig {
    /// Build the watermarks from the machine and the operator's setting.
    ///
    /// The policy lives here, not at the edge, because it is arithmetic with a
    /// correctness argument and requires explicit validation. The shell only
    /// supplies the two facts it can read: how much memory the process can use, and what
    /// `CELLD_MAX_RSS_MB` says. `None` for the setting means the default, and
    /// `Some(0)` disables pressure shedding altogether.
    ///
    /// The absolute cap is 95% of the machine and is never derived from the
    /// ceiling. A cap derived from the ceiling either lands above the machine
    /// (so it never fires) or below the ceiling (so it fires first on every
    /// sample and the classifier is reading the hard measurement again). Both
    /// have shipped in this file; neither is a floor.
    ///
    /// When the machine size is unknown, the cap falls back to 125% of an
    /// explicit ceiling. That is worse than a share of the machine and better
    /// than no floor at all.
    pub fn from_limits(total_memory_bytes: Option<u64>, max_rss_mb: Option<u64>) -> Self {
        // A watermark of zero is worse than no watermark: every sample crosses
        // it and none can clear it, so the node latches on its first sample and
        // refuses every cell for its lifetime. A machine that reports nothing
        // usable therefore gets no watermark at all. Linux reports 0 for a
        // cgroup limit of `0` and for an unreadable `MemTotal`.
        let usable = |bytes: u64| (bytes > 0).then_some(bytes);
        let high_bytes = match max_rss_mb {
            None => total_memory_bytes.and_then(|total| usable(total / 5 * 4)),
            Some(0) => None,
            Some(megabytes) => usable(megabytes.saturating_mul(1024 * 1024)),
        };
        let rss_hard_bytes = high_bytes.and_then(|high| {
            // `checked_mul`, because the fallback multiplies an operator's
            // number: a saturating multiply followed by a divide would return a
            // cap *below* the ceiling, which is the shape this avoids.
            let fallback = high.checked_mul(5).map_or(u64::MAX, |scaled| scaled / 4);
            let cap = total_memory_bytes
                .and_then(|total| usable(total / 100 * 95))
                .unwrap_or(fallback);
            usable(cap)
        });
        Self {
            high_bytes,
            rss_hard_bytes,
        }
    }

    /// Does the ceiling sit at or above the absolute cap?
    ///
    /// Then the cap is the effective limit and the node decides on its complete
    /// cgroup charge or RSS fallback. That is the operator's
    /// choice -- they asked for a ceiling above the safety floor -- but it
    /// gives up the recovery property of this module, so the shell reports it.
    pub fn ceiling_above_cap(self) -> bool {
        match (self.high_bytes, self.rss_hard_bytes) {
            (Some(high), Some(cap)) => high >= cap,
            _ => false,
        }
    }

    /// Fold a sample into the latches, and say why the node is shedding.
    ///
    /// Each ceiling latches on its own crossing: it engages at its ceiling and
    /// releases at its own low watermark, which is 80% of that ceiling. The
    /// hard cap is reported first, because it is the more serious of the two.
    ///
    /// The latches are carried separately and not derived from the reported
    /// reason. Sharing them lets one crossing hold the node against the other's
    /// watermark, in either direction: a crossing of the ordinary ceiling then
    /// holds the node on a resident-set watermark it did not cross,
    /// and a hard-cap crossing then holds it on a ceiling it never crossed.
    pub fn classify(self, s: Load, was: Latches) -> (Latches, Option<&'static str>) {
        let over = |ceiling: Option<u64>, sample: u64, latched: bool| {
            ceiling.is_some_and(|c| sample >= c || (latched && sample > Self::low_watermark(c)))
        };
        let latches = Latches {
            memory: over(self.high_bytes, s.memory_bytes(), was.memory),
            rss_hard: over(self.rss_hard_bytes, s.hard_bytes(), was.rss_hard),
        };
        let reason = if latches.rss_hard {
            Some(SHED_RSS_HARD)
        } else if latches.memory {
            Some(SHED_MEMORY)
        } else {
            None
        };
        (latches, reason)
    }

    /// The sample a latched ceiling releases at: 80% of the ceiling.
    ///
    /// One definition, because two callers read it. `classify` decides whether
    /// the latch still holds, and the walk down's stopping condition asks
    /// whether shedding can still get the sample down here. Two copies of the
    /// arithmetic drift, and a walk down that aims at the wrong number either
    /// stops above the line it could have reached or never stops at all.
    fn low_watermark(ceiling: u64) -> u64 {
        ceiling.saturating_mul(4) / 5
    }

    /// Whether the sample leaves the reserve below every configured low
    /// watermark. A rollout uses this stricter state instead of `!pressured`:
    /// a node can be below the high watermark without enough room to absorb a
    /// donor. A disabled ceiling imposes no headroom condition.
    pub fn has_headroom(self, sample: Load) -> bool {
        let below = |ceiling: Option<u64>, bytes: u64| {
            ceiling.is_none_or(|ceiling| bytes <= Self::low_watermark(ceiling))
        };
        below(self.high_bytes, sample.memory_bytes())
            && below(self.rss_hard_bytes, sample.hard_bytes())
    }

    /// Where the walk down against `metric` has to get the sample to.
    ///
    /// `None` means no ceiling is configured for that measurement, so nothing
    /// can latch on it and no walk down works against it.
    pub fn resume_line(self, metric: Metric) -> Option<u64> {
        let ceiling = match metric {
            Metric::InUse => self.high_bytes,
            Metric::Rss => self.rss_hard_bytes,
        };
        ceiling.map(Self::low_watermark)
    }

    /// Which measurement the walk down must work against, given the latches.
    ///
    /// The ordinary ceiling wins whenever it is latched, even if the cap is
    /// latched too. Eviction relieves the ordinary ceiling and may do nothing
    /// at all for the cap, so choosing the cap's measurement while the node is
    /// genuinely over its ordinary ceiling stops the walk down exactly when it
    /// is needed.
    pub fn walk_metric(latches: Latches) -> Metric {
        if latches.memory {
            Metric::InUse
        } else {
            Metric::Rss
        }
    }

    /// How far to shed down to. A memory trigger takes a proportion of what was
    /// just measured because the effect of an eviction is not visible until
    /// the next sample.
    pub fn release_target(resident_cells: usize) -> usize {
        resident_cells.saturating_sub((resident_cells / 10).max(1))
    }
}

pub const MAX_OUTBOUND_PIN_PERCENT: usize = 50;

/// May another cell be pinned resident by an outbound WebSocket?
///
/// An outbound socket is not hibernatable, so eviction refuses its cell for as
/// long as it is open. That is correct — a live host transport cannot survive
/// eviction — but it means every pinned cell is removed from the eviction
/// pool. Pin the whole ceiling and a resource walk down has nothing to
/// nominate. The budget is node-wide, counted in pinned *cells* rather than
/// sockets, because one socket is enough to pin. `ceiling` is the hard resident
/// cap.
pub fn may_pin_outbound(pinned_cells: usize, ceiling: Option<usize>) -> bool {
    ceiling.is_none_or(|cap| {
        // At least one, always. The share alone rounds to zero below a ceiling
        // of two, which would make an outbound socket impossible on a small
        // node rather than merely budgeted.
        pinned_cells < (cap.saturating_mul(MAX_OUTBOUND_PIN_PERCENT) / 100).max(1)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;
    const MIB: u64 = 1 << 20;

    // Goal: a container counts its cap until its engine reports, then what
    // it uses plus a quarter of its cap, never past the cap.
    #[test]
    fn a_container_counts_what_it_uses_plus_room() {
        let cap = 6 * GIB;
        assert_eq!(container_counted_bytes(cap, None), cap, "no report: the cap");
        assert_eq!(
            container_counted_bytes(cap, Some(300 * MIB)),
            300 * MIB + 1536 * MIB,
            "an idle computer"
        );
        assert_eq!(container_counted_bytes(cap, Some(5 * GIB)), cap, "never past the cap");
        assert_eq!(container_counted_bytes(cap, Some(u64::MAX)), cap);
        assert_eq!(container_counted_bytes(cap, Some(0)), cap / 4);
        assert_eq!(container_counted_bytes(256 * MIB, Some(10 * MIB)), 74 * MIB, "lite");
    }

    // The containers of a 12 GiB node's walk down: three idle computers
    // released at different times, and one whose object is resident.
    fn computers(now: u64) -> Vec<SleepCandidate<'static>> {
        let idle = |cell, quiet_for: u64| SleepCandidate {
            cell,
            counted_bytes: 2 * GIB,
            resident: false,
            last_active_mono_ms: now - quiet_for,
            stopping: false,
        };
        vec![
            idle("Computer:b", 10 * 60_000),
            idle("Computer:a", 30 * 60_000),
            SleepCandidate { resident: true, ..idle("Computer:busy", 60 * 60_000) },
            idle("Computer:c", 5 * 60_000),
        ]
    }

    // Goal: the least recently active idle container sleeps first, and
    // only as many as the walk down needs: one when one is enough, two
    // when the first does not cover it, and every idle one, no more, when
    // even that falls short.
    #[test]
    fn the_idlest_container_sleeps_first_and_only_as_many_as_needed() {
        let now = 100 * 60_000;
        let line = 8 * GIB;
        let sleep = |over: u64| containers_to_sleep(line + over, line, 0, &computers(now), now);
        assert_eq!(sleep(GIB), vec!["Computer:a"], "one covers a GiB over");
        assert_eq!(sleep(2 * GIB), vec!["Computer:a"], "exactly one's worth");
        assert_eq!(sleep(2 * GIB + 1), vec!["Computer:a", "Computer:b"], "a byte past one");
        assert_eq!(sleep(100 * GIB), vec!["Computer:a", "Computer:b", "Computer:c"], "all the idle ones, never the resident");
        assert_eq!(sleep(0), Vec::<&str>::new(), "at the line: none");
        let all = computers(now);
        let order: Vec<&str> = sleepable(&all, now).iter().map(|c| c.cell).collect();
        assert_eq!(order, ["Computer:a", "Computer:b", "Computer:c"], "what a refused start takes first: the idlest");
        assert_eq!(containers_to_sleep(line - 1, line, 0, &computers(now), now), Vec::<&str>::new(), "below it: none");
    }

    // Goal: a container whose object is resident (a socket, a request in
    // flight or waiting, a person watching) is never put to sleep, however
    // idle its cell looks and however far over the node is; nor one whose
    // cell did anything within CONTAINER_QUIET_MS, which is longer than an
    // object's save after its last activity (Fragment's 30 s).
    #[test]
    fn a_resident_or_recently_active_container_never_sleeps() {
        const { assert!(CONTAINER_QUIET_MS > 30_000) };
        let now = 100 * 60_000;
        let line = 8 * GIB;
        let resident: Vec<_> = computers(now).into_iter().map(|c| SleepCandidate { resident: true, ..c }).collect();
        assert_eq!(containers_to_sleep(line + 100 * GIB, line, 0, &resident, now), Vec::<&str>::new());
        let recent = |quiet_for| SleepCandidate { cell: "Computer:new", counted_bytes: 2 * GIB, resident: false, last_active_mono_ms: now - quiet_for, stopping: false };
        assert_eq!(containers_to_sleep(line + GIB, line, 0, &[recent(CONTAINER_QUIET_MS - 1)], now), Vec::<&str>::new(), "a millisecond inside the window");
        assert_eq!(containers_to_sleep(line + GIB, line, 0, &[recent(CONTAINER_QUIET_MS)], now), vec!["Computer:new"], "at its end");
        assert_eq!(containers_to_sleep(line + GIB, line, 0, &[recent(0)], 0), Vec::<&str>::new(), "a clock at its start");
    }

    // Goal: nothing sleeps while the evictions alone are projected to
    // reach the line; what they fall short by is what sleeps; and a stop
    // already under way counts as memory on its way back, so a sample
    // that still holds it asks for no second sleep. Replay: the same
    // sample asks the same.
    #[test]
    fn nothing_sleeps_when_evictions_reach_the_line() {
        let now = 100 * 60_000;
        let line = 8 * GIB;
        let all = computers(now);
        assert_eq!(containers_to_sleep(line + 3 * GIB, line, 3 * GIB, &all, now), Vec::<&str>::new(), "evictions cover it");
        assert_eq!(containers_to_sleep(line + 3 * GIB, line, u64::MAX, &all, now), Vec::<&str>::new());
        assert_eq!(containers_to_sleep(line + 3 * GIB, line, 2 * GIB, &all, now), vec!["Computer:a"], "a GiB short");
        let mut stopping = all.clone();
        stopping[1].stopping = true;
        assert_eq!(containers_to_sleep(line + 2 * GIB, line, 0, &stopping, now), Vec::<&str>::new(), "its sleep under way covers it");
        assert_eq!(containers_to_sleep(line + 3 * GIB, line, 0, &stopping, now), vec!["Computer:b"], "the next idlest for the rest");
        assert_eq!(containers_to_sleep(line + 3 * GIB, line, 0, &stopping, now), containers_to_sleep(line + 3 * GIB, line, 0, &stopping, now));
    }

    // Goal: on a 12 GiB machine, at the default ceiling, idle computers
    // counted by use leave room where their caps did not.
    #[test]
    fn idle_computers_fit_where_caps_did_not() {
        let config = PressureConfig::from_limits(Some(12 * GIB), None);
        let node = |containers: u64| Load {
            resident_cells: 1,
            rss_bytes: 400 * MIB,
            in_use_bytes: 400 * MIB,
            cgroup_working_set_bytes: None,
            cgroup_current_bytes: None,
            container_reserved_bytes: containers,
        };
        let shed = |load| config.classify(load, Latches::default()).1;
        let caps = |n: u64| n * container_counted_bytes(6 * GIB, None);
        let used = |n: u64| n * container_counted_bytes(6 * GIB, Some(300 * MIB));
        assert_eq!(shed(node(caps(1))), None);
        assert!(shed(node(caps(2))).is_some(), "two caps: shedding, so no third");
        assert_eq!(shed(node(used(5))), None, "five idle computers");
        assert_eq!(shed(node(used(6))), Some(SHED_MEMORY));
    }
}
