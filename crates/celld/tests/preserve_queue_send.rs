//! A queue send from a fetch still answering when a same-node restart
//! (`POST /shutdown?handoff=preserve`) begins: the broker takes it, and the
//! next process delivers it. Refused, the message was gone, and its producer
//! (a paid call's charge, say) had nothing to send again.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use support::Dev;

const CONFIG: &str = r#"{
  "name": "test",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "no_bundle": true,
  "durable_objects": { "bindings": [{ "name": "SEEN", "class_name": "Seen" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Seen"] }],
  "queues": {
    "producers": [{ "binding": "JOBS", "queue": "jobs" }],
    "consumers": [{ "queue": "jobs", "max_batch_timeout": 1 }]
  }
}"#;

const WORKER: &str = r#"
export class Seen {
  constructor(ctx) {
    this.ctx = ctx;
  }

  async fetch(request) {
    const seen = (await this.ctx.storage.get("seen")) ?? [];
    if (request.method === "POST") {
      seen.push(await request.text());
      await this.ctx.storage.put("seen", seen);
    }
    return Response.json(seen);
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/send") {
      const after = Number(url.searchParams.get("after"));
      await new Promise((resolve) => setTimeout(resolve, after));
      try {
        await env.JOBS.send(url.searchParams.get("body"));
        return new Response("sent");
      } catch (error) {
        return new Response(`refused: ${error}`, { status: 503 });
      }
    }
    return env.SEEN.getByName("seen").fetch("http://seen/");
  },

  async queue(batch, env) {
    const seen = env.SEEN.getByName("seen");
    for (const message of batch.messages) {
      await seen.fetch("http://seen/", { method: "POST", body: message.body });
    }
  },
};
"#;

/// The node's operator listener, from the line it announces it with.
fn internal(dev: &Dev) -> String {
    let output = dev.output();
    let (_, rest) = output
        .split_once("celld internal listening on ")
        .unwrap_or_else(|| panic!("no operator listener announced:\n{output}"));
    rest.split_whitespace().next().unwrap().to_string()
}

fn post(address: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(address).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    write!(stream, "POST {path} HTTP/1.0\r\nhost: localhost\r\ncontent-length: 0\r\n\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

/// Waits until the consumer has stored `body`.
fn delivered(dev: &Dev, body: &str) {
    let until = Instant::now() + Duration::from_secs(30);
    loop {
        let (seen, _) = dev.json("/seen");
        if seen.as_array().unwrap().iter().any(|seen| seen == body) {
            return;
        }
        assert!(Instant::now() < until, "{body} not delivered: {seen}\n{}", dev.output());
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn a_queue_send_during_a_preserve_drain_is_kept_and_delivered() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(dir.path().join("index.js"), WORKER).unwrap();

    let mut dev = Dev::start(dir.path());
    // The broker is resident, as on a node that has sent before.
    assert_eq!(dev.get("/send?after=0&body=warm").unwrap(), (200, "sent".into()));
    delivered(&dev, "warm");

    let operator = internal(&dev);
    let late = std::thread::scope(|scope| {
        let sending = scope.spawn(|| dev.get("/send?after=1500&body=late").unwrap());
        std::thread::sleep(Duration::from_millis(300));
        let shutdown = post(&operator, "/shutdown?handoff=preserve");
        assert!(shutdown.starts_with("HTTP/1.0 200") || shutdown.starts_with("HTTP/1.1 200"), "{shutdown}");
        sending.join().unwrap()
    });
    assert_eq!(late, (200, "sent".into()), "the send while the drain ran\n{}", dev.output());

    let until = Instant::now() + Duration::from_secs(40);
    while dev.exited().is_none() {
        assert!(Instant::now() < until, "the node did not stop:\n{}", dev.output());
        std::thread::sleep(Duration::from_millis(100));
    }
    drop(dev);

    // The next process of the same node has the message, and delivers it.
    let dev = Dev::start(dir.path());
    delivered(&dev, "late");
}
