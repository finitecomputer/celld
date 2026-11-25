//! A Durable Object's continuation outlives the event that registered it, as
//! in workerd, where an actor's requests share one I/O context: a promise
//! chain an ended event left behind, let go by a later event, still runs its
//! timers and storage. A container's `monitor().then(...)` is one: its exit
//! is heard in a later event, and the restart it asks for awaits its own I/O.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use std::time::{Duration, Instant};

use support::{project, Dev};

const WORKER: &str = r#"
export class Keeper {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.searchParams.has("arm")) {
      // a continuation of this event, run once a later event lets it go
      new Promise((resolve) => (this.letGo = resolve)).then(async () => {
        await new Promise((resolve) => setTimeout(resolve, 50));
        await this.ctx.storage.put("ran", true);
      });
      return new Response("armed");
    }
    if (url.searchParams.has("go")) {
      this.letGo();
      return new Response("let go");
    }
    return Response.json({ ran: (await this.ctx.storage.get("ran")) ?? false });
  }
}

export default {
  fetch(request, env) {
    return env.KEEPER.getByName("one").fetch(request);
  },
};
"#;

#[test]
fn a_continuation_of_an_ended_event_runs_its_io() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("KEEPER", "Keeper")]);
    let dev = Dev::start(dir.path());

    let (status, body) = dev.get("/?arm").unwrap();
    assert_eq!((status, body.as_str()), (200, "armed"), "{}", dev.output());
    // the arming event has ended, and nothing of it is pending
    std::thread::sleep(Duration::from_millis(500));
    let (status, body) = dev.get("/?go").unwrap();
    assert_eq!((status, body.as_str()), (200, "let go"), "{}", dev.output());

    // its timer fires and its write lands, though its own event is over
    let deadline = Instant::now() + Duration::from_secs(10);
    let seen = loop {
        let (seen, _) = dev.json("/");
        if seen["ran"] == true || Instant::now() >= deadline {
            break seen;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(seen["ran"], true, "the ended event's continuation ran its I/O:\n{}", dev.output());
}
