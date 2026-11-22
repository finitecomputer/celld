//! `put(key, value, { allowUnconfirmed: true })`: stored exactly as a
//! default put is, in the same order beside other operations (only the
//! gate's wait differs), and one made inside a transaction, even through
//! the root handle, goes when the transaction is rolled back.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use support::{project, Dev};

const WORKER: &str = r#"
export class Counter {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    const op = new URL(request.url).searchParams.get("op");
    const storage = this.ctx.storage;
    const options = new URL(request.url).searchParams.has("unconfirmed") ? { allowUnconfirmed: true } : undefined;
    if (op === "same-turn") {
      await storage.put("count", 1);
      // not awaited: what the next line, in the same turn, reads
      storage.put("count", 7, options);
      const now = storage.kv.get("count");
      return Response.json({ now: now ?? null, later: (await storage.get("count")) ?? null });
    }
    if (op === "delete-then-put") {
      await storage.put("x", 1);
      const deleted = storage.delete("x");
      const put = storage.put("x", 7, options);
      await Promise.all([deleted, put]);
      return Response.json({ x: (await storage.get("x")) ?? null });
    }
    if (op === "rolled-back") {
      try {
        storage.transactionSync(() => {
          storage.put("lost", 1, { allowUnconfirmed: true });
          throw new Error("roll back");
        });
      } catch (_) {}
      await new Promise((resolve) => setTimeout(resolve, 10));
      return Response.json({ lost: (await storage.get("lost")) ?? null });
    }
    return new Response("op?", { status: 400 });
  }
}

export default {
  fetch(request, env) {
    return env.COUNTER.getByName("one").fetch(request);
  },
};
"#;

fn dev() -> (tempfile::TempDir, Dev) {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("COUNTER", "Counter")]);
    let dev = Dev::start(dir.path());
    (dir, dev)
}

/// What a default put of the same op stores, and what an unconfirmed one does.
fn both(dev: &Dev, op: &str) -> (serde_json::Value, serde_json::Value) {
    let (default, _) = dev.json(&format!("/?op={op}"));
    let (unconfirmed, _) = dev.json(&format!("/?op={op}&unconfirmed"));
    (default, unconfirmed)
}

#[test]
fn an_unconfirmed_put_is_ordered_as_a_default_put() {
    let (_dir, dev) = dev();
    for op in ["same-turn", "delete-then-put"] {
        let (default, unconfirmed) = both(&dev, op);
        assert_eq!(unconfirmed, default, "{op}: {}", dev.output());
    }
}

#[test]
fn an_unconfirmed_put_never_outlives_its_transactions_rollback() {
    let (_dir, dev) = dev();
    let (seen, _) = dev.json("/?op=rolled-back");
    assert_eq!(seen, serde_json::json!({ "lost": null }), "{}", dev.output());
}

