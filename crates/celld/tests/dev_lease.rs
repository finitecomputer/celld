//! A `celld dev` node rides out a stall of its lease's store longer than a
//! fleet's lease (10 s), as a busy disk makes one; an operator's
//! `CELLD_TTL_MS` still rules it, and at 10 s the same stall fences it.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use std::path::Path;
use std::time::{Duration, Instant};

use support::{project, Dev};

const WORKER: &str = r#"
export class Counter {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch() {
    const n = ((await this.ctx.storage.get("n")) ?? 0) + 1;
    await this.ctx.storage.put("n", n);
    return Response.json({ n });
  }
}

export default {
  fetch(request, env) {
    return env.COUNTER.getByName("one").fetch(request);
  },
};
"#;

/// Longer than a fleet's lease, as the stall that fenced fragment's e2e
/// node was (one renewal held 2.5 s, then its read-back, until the 10 s
/// lease ran out).
const STALL: Duration = Duration::from_secs(15);

/// Holds the write lock of every database of the node's store (the lease's
/// among them, whichever file keeps it), as a commit stuck on a busy disk
/// holds it, for `STALL` or until the node exits. Answers whether it exited.
fn stall_the_store(dev: &mut Dev, project: &Path) -> Option<std::process::ExitStatus> {
    let state = project.join(".celld/dev");
    let held: Vec<rusqlite::Connection> = ["objects.sqlite3", "leases.sqlite3"]
        .into_iter()
        .map(|name| state.join(name))
        .filter(|path| path.exists())
        .map(|path| {
            let connection = rusqlite::Connection::open(&path).unwrap();
            connection.busy_timeout(Duration::from_secs(30)).unwrap();
            connection.execute_batch("BEGIN IMMEDIATE").unwrap();
            connection
        })
        .collect();
    assert!(!held.is_empty(), "no store under {}", state.display());
    let until = Instant::now() + STALL;
    while Instant::now() < until {
        if let Some(status) = dev.exited() {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    for connection in held {
        connection.execute_batch("ROLLBACK").unwrap();
    }
    // a renewal and its read-back land within a cadence of the release
    std::thread::sleep(Duration::from_secs(3));
    dev.exited()
}

/// Goal: a dev node whose store stalls past a fleet's lease keeps its lease
/// and serves on once the store answers, as Cloudflare never fences a busy
/// isolate. Invalid before: the node fenced itself 10 s into the stall
/// ("SELF-FENCE: node lease not renewed within TTL") and the stack exited.
#[test]
fn a_dev_node_rides_out_a_stalled_store() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("COUNTER", "Counter")]);
    let mut dev = Dev::start(dir.path());
    let (seen, _) = dev.json("/");
    assert_eq!(seen["n"], 1);

    if let Some(status) = stall_the_store(&mut dev, dir.path()) {
        panic!(
            "celld dev exited with {status} during a {STALL:?} stall of its store:\n{}",
            dev.output()
        );
    }
    let (seen, _) = dev.json("/");
    assert_eq!(seen["n"], 2, "the object after the stall:\n{}", dev.output());
    assert!(!dev.output().contains("SELF-FENCE"), "{}", dev.output());
}

/// Goal: the dev lease is a default, not a cut of the fence. An operator's
/// `CELLD_TTL_MS` rules a dev node, and at a fleet's 10 s the same stall
/// still fences it, saying so.
#[test]
fn an_operators_lease_still_fences_a_stalled_dev_node() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("COUNTER", "Counter")]);
    let mut dev = Dev::start_with(dir.path(), &[("CELLD_TTL_MS", "10000")]);
    let (seen, _) = dev.json("/");
    assert_eq!(seen["n"], 1);

    let exited = stall_the_store(&mut dev, dir.path());
    assert!(
        exited.is_some_and(|status| !status.success()),
        "a dev node at a 10 s lease outlived a {STALL:?} stall: {exited:?}\n{}",
        dev.output()
    );
    assert!(
        dev.output().contains("SELF-FENCE: node lease not renewed within TTL"),
        "{}",
        dev.output()
    );
}
