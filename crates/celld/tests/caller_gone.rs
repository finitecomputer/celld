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
    return Response.json({
      began: (await this.ctx.storage.get("began")) ?? false,
      done: (await this.ctx.storage.get("done")) ?? false,
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
