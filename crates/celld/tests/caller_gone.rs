//! A Durable Object's handler whose caller hangs up runs on to its end, as
//! workerd's does: an actor's pending I/O is the object's, not the request's
//! (workerd's `IoContext::IncomingRequest::drain`), so a timer it awaits
//! still fires, and what it writes after is written.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use std::io::Write as _;
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};

use support::{project, Dev};

const WORKER: &str = r#"
export class Saver {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.searchParams.has("work")) {
      await this.ctx.storage.put("began", true);
      // two seconds of waits, then a write: its caller is gone by then
      for (let i = 0; i < 20; i++) await new Promise((resolve) => setTimeout(resolve, 100));
      await this.ctx.storage.put("done", true);
      return new Response("done");
    }
    if (url.searchParams.has("wait")) {
      // leaves a write for after the budget, then waits for another request
      setTimeout(() => this.ctx.storage.put("late", true), 4000);
      await this.ctx.storage.put("waiting", true);
      await new Promise((resolve) => (this.release = resolve));
      return new Response("released");
    }
    if (url.searchParams.has("release")) {
      this.release();
      return new Response("let go");
    }
    if (url.searchParams.has("forever")) {
      // counts every 100 ms and never answers
      for (let n = 1; ; n++) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        await this.ctx.storage.put("count", n);
      }
    }
    return Response.json({
      began: (await this.ctx.storage.get("began")) ?? false,
      done: (await this.ctx.storage.get("done")) ?? false,
      count: (await this.ctx.storage.get("count")) ?? 0,
      waiting: (await this.ctx.storage.get("waiting")) ?? false,
      late: (await this.ctx.storage.get("late")) ?? false,
    });
  }
}

export default {
  fetch(request, env) {
    return env.SAVER.getByName("one").fetch(request);
  },
};
"#;

/// Polls the object until `want` holds, within `within`.
fn until(dev: &Dev, within: Duration, want: impl Fn(&serde_json::Value) -> bool) -> serde_json::Value {
    let deadline = Instant::now() + within;
    loop {
        let (seen, _) = dev.json("/");
        if want(&seen) || Instant::now() >= deadline {
            return seen;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn a_durable_objects_handler_runs_on_after_its_caller_hangs_up() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("SAVER", "Saver")]);
    let dev = Dev::start(dir.path());

    // its caller asks, through the Worker, and hangs up once the work began
    let mut caller = TcpStream::connect(("127.0.0.1", dev.port)).unwrap();
    write!(caller, "GET /?work HTTP/1.0\r\nhost: 127.0.0.1\r\n\r\n").unwrap();
    let began = until(&dev, Duration::from_secs(20), |seen| seen["began"] == true);
    assert_eq!(began["began"], true, "the work began:\n{}", dev.output());
    assert_eq!(began["done"], false, "the work is still under way as its caller hangs up:\n{}", dev.output());
    caller.shutdown(Shutdown::Both).unwrap();
    drop(caller);

    // its waits end and its write lands, with no caller left to answer
    let seen = until(&dev, Duration::from_secs(15), |seen| seen["done"] == true);
    assert_eq!(seen["done"], true, "the handler ran on after its caller hung up:\n{}", dev.output());
}

/// The handler budget, in seconds, for the test of a handler that never ends.
const BUDGET_S: u64 = 3;

#[test]
fn a_handler_whose_caller_hung_up_still_ends_at_its_budget() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("SAVER", "Saver")]);
    let dev = Dev::start_with(dir.path(), &[("CELLD_HANDLER_BUDGET_S", &BUDGET_S.to_string())]);

    let mut caller = TcpStream::connect(("127.0.0.1", dev.port)).unwrap();
    write!(caller, "GET /?forever HTTP/1.0\r\nhost: 127.0.0.1\r\n\r\n").unwrap();
    let counting = until(&dev, Duration::from_secs(20), |seen| seen["count"].as_u64().is_some_and(|n| n > 0));
    assert!(counting["count"].as_u64().is_some_and(|n| n > 0), "it counts:\n{}", dev.output());
    caller.shutdown(Shutdown::Both).unwrap();
    drop(caller);

    // past its budget it counts no more, as a handler whose caller waits
    std::thread::sleep(Duration::from_secs(BUDGET_S + 2));
    let (first, _) = dev.json("/");
    std::thread::sleep(Duration::from_secs(1));
    let (then, _) = dev.json("/");
    assert_eq!(first["count"], then["count"], "the handler ended at its budget:\n{}", dev.output());
}

#[test]
fn a_hung_up_handler_settled_by_another_request_keeps_what_it_left() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("SAVER", "Saver")]);
    let dev = Dev::start_with(dir.path(), &[("CELLD_HANDLER_BUDGET_S", &BUDGET_S.to_string())]);

    let mut caller = TcpStream::connect(("127.0.0.1", dev.port)).unwrap();
    write!(caller, "GET /?wait HTTP/1.0\r\nhost: 127.0.0.1\r\n\r\n").unwrap();
    let waiting = until(&dev, Duration::from_secs(20), |seen| seen["waiting"] == true);
    assert_eq!(waiting["waiting"], true, "it waits:\n{}", dev.output());
    caller.shutdown(Shutdown::Both).unwrap();
    drop(caller);
    std::thread::sleep(Duration::from_millis(300));

    // another request settles it within its budget; the write it left for
    // after that budget is the object's, and lands
    let (status, body) = dev.get("/?release").unwrap();
    assert_eq!((status, body.as_str()), (200, "let go"), "{}", dev.output());
    let seen = until(&dev, Duration::from_secs(10), |seen| seen["late"] == true);
    assert_eq!(seen["late"], true, "what it left after settling ran past the budget:\n{}", dev.output());
}
