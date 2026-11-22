//! `put(key, value, { allowUnconfirmed: true })`, as workerd takes it: the
//! write lands in the same turn as the call, and one made inside a
//! transaction, even through the root handle, goes when the transaction is
//! rolled back.

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
    if (op === "same-turn") {
      // not awaited: the next line, in the same turn, reads it back
      storage.put("count", 7, { allowUnconfirmed: true });
      const now = storage.kv.get("count");
      return Response.json({ now: now ?? null, later: (await storage.get("count")) ?? null });
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

#[test]
fn an_unconfirmed_put_lands_in_its_own_turn() {
    let (_dir, dev) = dev();
    let (seen, _) = dev.json("/?op=same-turn");
    assert_eq!(seen, serde_json::json!({ "now": 7, "later": 7 }), "{}", dev.output());
}

#[test]
fn an_unconfirmed_put_never_outlives_its_transactions_rollback() {
    let (_dir, dev) = dev();
    let (seen, _) = dev.json("/?op=rolled-back");
    assert_eq!(seen, serde_json::json!({ "lost": null }), "{}", dev.output());
}
