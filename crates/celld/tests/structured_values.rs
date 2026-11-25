//! Maps and Sets, nested in each other, come back whole from every path that
//! clones a value with V8's serializer: `structuredClone` (with the host
//! objects it revives through JavaScript, Blob, File and DOMException, nested
//! in them too), Durable Object storage (read again after a restart) and RPC.
//! V8 15.8.62.68 builds a deserialized Map or Set in C++ and enforces
//! no-JavaScript scopes on API callbacks, so these are the paths it changed.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use support::{project, Dev};

const WORKER: &str = r#"
import { DurableObject } from "cloudflare:workers";

// Maps and Sets in each other, in objects and arrays, one keyed by an object.
const value = () => ({
  map: new Map([
    ["a", 1],
    ["set", new Set([1, "two", 3.5])],
    ["map", new Map([[{ k: 1 }, new Set([new Map([["deep", [1, 2]]])])]])],
  ]),
  set: new Set([new Map([["x", new Set(["y"])]]), [new Set([4])]]),
  empty: [new Map(), new Set()],
});

// Host objects the clone revives through JavaScript, at the top and nested.
const hosts = () => ({
  ...value(),
  blob: new Blob(["hi"], { type: "text/plain" }),
  file: new File(["f"], "f.txt", { type: "text/x", lastModified: 5 }),
  error: new DOMException("m", "DataCloneError"),
  nested: new Map([["blobs", new Set([new Blob(["deep"]), new File(["g"], "g.txt", { lastModified: 7 })])]]),
});

// What JSON can say about a value, its Maps, Sets and host objects named.
const describe = async (v) => {
  if (v instanceof Map) {
    const entries = [];
    for (const [k, x] of v) entries.push([await describe(k), await describe(x)]);
    return { Map: entries };
  }
  if (v instanceof Set) {
    const items = [];
    for (const x of v) items.push(await describe(x));
    return { Set: items };
  }
  if (v instanceof File)
    return { File: { name: v.name, type: v.type, lastModified: v.lastModified, text: await v.text() } };
  if (v instanceof Blob) return { Blob: { type: v.type, text: await v.text() } };
  if (v instanceof DOMException) return { DOMException: { name: v.name, message: v.message } };
  if (Array.isArray(v)) return Promise.all(v.map(describe));
  if (v && typeof v === "object") {
    const out = {};
    for (const [k, x] of Object.entries(v)) out[k] = await describe(x);
    return out;
  }
  return v;
};

const refusal = (f) => {
  try {
    f();
    return null;
  } catch (e) {
    return e?.name ?? String(e);
  }
};

export class Box extends DurableObject {
  async fetch(request) {
    switch (new URL(request.url).pathname) {
      case "/put":
        await this.ctx.storage.put("v", value());
        return Response.json(true);
      case "/get":
        return Response.json(await describe(await this.ctx.storage.get("v")));
    }
    return new Response("where?", { status: 404 });
  }

  echo(v) {
    return v;
  }
}

export default {
  async fetch(request, env) {
    switch (new URL(request.url).pathname) {
      case "/expected":
        return Response.json({ value: await describe(value()), hosts: await describe(hosts()) });
      case "/clone": {
        const original = hosts();
        const clone = structuredClone(original);
        return Response.json({
          clone: await describe(clone),
          // a copy, not the same objects
          fresh: clone.map !== original.map && clone.nested.get("blobs") !== original.nested.get("blobs"),
          // still refused: a type with no clone, and a function
          request: refusal(() => structuredClone(new Request("https://x.invalid/"))),
          function: refusal(() => structuredClone({ f() {} })),
        });
      }
      case "/rpc":
        return Response.json(await describe(await env.BOX.getByName("b").echo(value())));
      case "/put":
      case "/get":
        return env.BOX.getByName("b").fetch(request);
    }
    return new Response("where?", { status: 404 });
  },
};
"#;

// Goal: a value of nested Maps and Sets comes back as it went in from
// structuredClone, storage and RPC; structuredClone revives Blob, File and
// DOMException at any depth, gives copies, and still refuses what has no
// clone. Replay: asked twice, the same answers. Restart: the stored value
// reads back the same from a new node.
#[test]
fn maps_sets_and_host_objects_clone_store_and_cross_rpc_whole() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[("BOX", "Box")]);
    let dev = Dev::start(dir.path());

    let (expected, _) = dev.json("/expected");
    // the description itself says what it should: nothing collapsed to {}
    assert_eq!(
        expected["value"]["map"]["Map"][2][1]["Map"][0][1]["Set"][0]["Map"][0],
        serde_json::json!(["deep", [1, 2]]),
        "{expected}"
    );
    let file = &expected["hosts"]["nested"]["Map"][0][1]["Set"][1]["File"];
    assert_eq!((&file["name"], &file["text"]), (&serde_json::json!("g.txt"), &serde_json::json!("g")), "{expected}");

    for _ in 0..2 {
        let (clone, _) = dev.json("/clone");
        assert_eq!(clone["clone"], expected["hosts"], "{}", dev.output());
        assert_eq!(clone["fresh"], true);
        assert_eq!(clone["request"], "DataCloneError");
        assert_eq!(clone["function"], "DataCloneError");

        let (rpc, _) = dev.json("/rpc");
        assert_eq!(rpc, expected["value"], "{}", dev.output());

        assert_eq!(dev.json("/put").0, true);
        let (stored, _) = dev.json("/get");
        assert_eq!(stored, expected["value"], "{}", dev.output());
    }
    drop(dev);

    let dev = Dev::start(dir.path());
    let (stored, _) = dev.json("/get");
    assert_eq!(stored, expected["value"], "after a restart:\n{}", dev.output());
}
