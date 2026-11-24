# `celld dev` fences itself when its disk is busy

Measured on the fork's `selfhost` (98d5272, v0.6.3 and the fork's fixes),
2026-11-11, on one Linux box: btrfs `/home` on NVMe, 32 cores.

## What happens

`celld dev` renews its node lease every ~3.3 s against a 10 s TTL. A
renewal is a compare-and-swap on the development store, a SQLite file at
`PROJECT/.celld/dev/objects.sqlite3`, bounded at 2.5 s. A timed-out
renewal is ambiguous, and the node then reads its lease back, with no
bound. When neither returns before the TTL, the watchdog fences the node
(`SELF-FENCE: node lease not renewed within TTL`, exit 3) and the dev
stack stops.

## The cause: the store's disk, not a thread

- **Not the runtime's threads.** The renewal runs on the core thread
  (`celld-core`), off the host workers. The store's calls run on Tokio's
  blocking pool (512 threads). In the box's fence on 2026-10-22, every
  store user stalled together: the renewal (2.5 s, then its read-back,
  which never returned), a listing (`posture repair failed … list`), and
  the LTX ship loop (5.5 s idle).
- **Disk writeback alone does it.** `run.sh` runs an idle `celld dev`
  with its state on this disk while `dd` writes 16 GB to the same disk
  and fsyncs. The load average stayed between 2 and 5. The node fenced
  within 20 s in each of three runs: a release build, a debug build, and
  a debug build with `NORMAL` (below). The renewals timed out at 2.5 s
  and the read-backs never landed. The stock v0.6.3 release binary
  fenced in two runs of three. In the third, one renewal timed out and
  the node survived.
- **Every SQLite call waits, read or write, whatever its `synchronous`.**
  `probe.py` timed one call every 200 ms, each on a fresh connection as
  the store makes them, beside 16 GB of writeback:

  | call | slowest |
  |---|---|
  | write, `synchronous=FULL` (the store's setting) | 10.8 s |
  | write, `synchronous=NORMAL` | 10.8 s |
  | read, WAL | 3.6 s |

  A `celld dev` built with `NORMAL` fenced as well. So the commit's fsync
  is not the cause: the filesystem holds every writer, and some readers,
  while it writes back.
- **Off that disk, nothing stalls.** With the project, and so its state,
  on tmpfs and the same load on `/home`, 17 renewals took at most 1 ms
  and the node stayed up.

## What would and would not help

- **A dedicated thread, a priority, or a fairer timer would not.** The
  timer fires on time and the attempt starts on time, then waits on the
  file system.
- **Keep the dev state off a busy disk.** That works today. For a stack
  that throws its state away (an e2e run), tmpfs is enough.
- **In celld,** two options. Neither is a contained fix, so both are left
  to upstream:
  - a single dev node, whose only rival is the next node of a reload,
    could take a longer TTL;
  - the dev lease could live outside the shared SQLite file.

  `celld dev` offers no setting for its lease today. The second is now
  on this fork, for the busy node below.

## A busy node: the renewal waits for the node's own writes

Seen 2026-11-24 in fragment's e2e on this fork's `selfhost` (da094d8),
with computers in krun microVMs: one fence in 40 minutes, while other
builds shared the disk.

- **Writers waited and readers did not.** Five renewals in a row timed
  out (19:33:22 to 19:33:29, at 2.5 s and then at shrinking bounds) and
  the node fenced. Halfway through, a read of the fleet sample in the
  same file answered in 3 ms. Worker requests waited 4 to 12 s, and LTX
  durability proofs timed out.
- **One writer per database.** Every write to the store is a `BEGIN
  IMMEDIATE` and a `synchronous=FULL` commit, so it keeps SQLite's write
  lock through its fsync. The leases shared the file with every LTX, R2
  and deployment write, so a renewal waited for each write ahead of it.
  SQLite's busy handler is no queue: a waiter sleeps up to 100 ms between
  tries and loses the lock to writers that come back sooner.
- **Measured.** `lanes.py` emulates the store on this disk: writer
  threads put LTX-sized objects back to back (some of them large), a
  renewal every 3.33 s, 40 s a run. The renewal's own statements and
  commit took under 30 ms in every run; the rest was the wait.

  | writers | lease | lock wait, slowest | renewal p90 | renewal, slowest |
  |---|---|---|---|---|
  | 16, 5% at 8 MiB | among the objects | 1429 ms | 1335 ms | 1437 ms |
  | 16, 5% at 8 MiB | its own file | 0 | 24 ms | 27 ms |
  | 4, 2% at 4 MiB, beside a celld release build | among the objects | 1130 ms | 229 ms | 1157 ms |
  | 4, 2% at 4 MiB, beside a celld release build | its own file | 0 | 22 ms | 39 ms |

- **Fixed on this fork.** The leases (`nodes/`) have their own database,
  `leases.sqlite3` beside `objects.sqlite3`, so a renewal waits only for
  another lease write. A store from before moves its leases over when it
  opens. This does not help the idle node above: there the renewal's own
  commit waits on the file system.
