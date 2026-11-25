//! fork: the Cache API keeps what Cloudflare's keeps, per node: a response
//! one isolate puts is the response another matches, until its lifetime
//! ends or it is deleted; a response its headers say not to keep is not
//! kept; named caches, and Dynamic Workers, keep their own (a Dynamic
//! Worker's, nothing). A node started again has an empty cache.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use support::Dev;

const WORKER: &str = r#"
import { DurableObject } from "cloudflare:workers";

const KEY = (k) => "https://cache.test/" + k;

// What a match answers: hit or not, and the response's parts.
const seen = async (found) => found === undefined
  ? { hit: false }
  : { hit: true, status: found.status, body: await found.text(), type: found.headers.get("content-type"), mine: found.headers.get("x-mine"), cookie: found.headers.get("set-cookie") };

const put = async (cache, k, status, headers) => {
  await cache.put(KEY(k), new Response(status === 204 || status === 304 ? null : "body of " + k, { status, headers: { "content-type": "text/plain", ...headers } }));
};

// A Durable Object puts, so its isolate is not the stateless one that
// matches.
export class Putter extends DurableObject {
  async fetch(request) {
    const k = new URL(request.url).searchParams.get("k");
    await put(caches.default, k, 200, { "cache-control": "max-age=300" });
    return new Response("put");
  }
}

const LOADED = `
export default {
  async fetch(request) {
    const key = "https://cache.test/loaded";
    await caches.default.put(key, new Response("loaded", { headers: { "cache-control": "max-age=300" } }));
    const own = await caches.default.match(key);
    const platforms = await caches.default.match("https://cache.test/a");
    return Response.json({ own: own !== undefined, platforms: platforms !== undefined });
  },
};
`;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const k = url.searchParams.get("k");
    const cc = url.searchParams.get("cc");
    const status = Number(url.searchParams.get("status") ?? 200);
    switch (url.pathname) {
      case "/put": {
        const headers = {};
        if (cc !== null) headers["cache-control"] = cc;
        if (url.searchParams.has("cookie")) headers["set-cookie"] = "session=1";
        if (url.searchParams.has("mine")) headers["x-mine"] = "dropped";
        await put(caches.default, k, status, headers);
        return new Response("put");
      }
      case "/put-in-object":
        return env.PUTTER.getByName("p").fetch(request);
      case "/match":
        return Response.json(await seen(await caches.default.match(KEY(k))));
      case "/match-post":
        return Response.json({
          plain: await seen(await caches.default.match(new Request(KEY(k), { method: "POST" }))),
          ignoring: await seen(await caches.default.match(new Request(KEY(k), { method: "POST" }), { ignoreMethod: true })),
        });
      case "/match-partial":
        return Response.json({
          range: await seen(await caches.default.match(new Request(KEY(k), { headers: { range: "bytes=0-1" } }))),
          etag: await seen(await caches.default.match(new Request(KEY(k), { headers: { "if-none-match": "\"x\"" } }))),
          since: await seen(await caches.default.match(new Request(KEY(k), { headers: { "if-modified-since": "Sun, 09 Sep 2001 01:46:40 GMT" } }))),
          whole: await seen(await caches.default.match(new Request(KEY(k)))),
        });
      case "/delete":
        return Response.json(await caches.default.delete(KEY(k)));
      case "/named": {
        const mine = await caches.open("mine");
        await put(mine, k, 200, { "cache-control": "max-age=300" });
        return Response.json({ named: await seen(await mine.match(KEY(k))), default: await seen(await caches.default.match(KEY(k))) });
      }
      case "/loaded":
        return env.LOADER.get("tenant", () => ({ compatibilityDate: "2026-01-01", mainModule: "app.js", modules: { "app.js": LOADED } })).getEntrypoint().fetch(request);
    }
    return new Response("where?", { status: 404 });
  },
};
"#;

const CONFIG: &str = r#"{
  "name": "test",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "no_bundle": true,
  "durable_objects": { "bindings": [{ "name": "PUTTER", "class_name": "Putter" }] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Putter"] }],
  "worker_loaders": [{ "binding": "LOADER" }]
}"#;

fn project(dir: &std::path::Path) {
    std::fs::write(dir.join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(dir.join("index.js"), WORKER).unwrap();
}

fn hit(dev: &Dev, k: &str) -> serde_json::Value {
    dev.json(&format!("/match?k={k}")).0
}

// Goal: a response a Durable Object puts is matched by a stateless request,
// with its status, body and headers, again and again (replay); deleted, it
// is gone, and a second delete says so. Restart: a node started again on
// the same project has an empty cache.
#[test]
fn a_put_is_matched_across_the_nodes_isolates() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path());
    {
        let dev = Dev::start(dir.path());
        assert_eq!(hit(&dev, "a"), serde_json::json!({ "hit": false }), "nothing before a put");
        let (status, body) = dev.get("/put-in-object?k=a").unwrap();
        assert_eq!((status, body.as_str()), (200, "put"));
        for _ in 0..3 {
            assert_eq!(
                hit(&dev, "a"),
                serde_json::json!({ "hit": true, "status": 200, "body": "body of a", "type": "text/plain", "mine": null, "cookie": null }),
                "{}",
                dev.output()
            );
        }
        assert_eq!(dev.json("/delete?k=a").0, true);
        assert_eq!(dev.json("/delete?k=a").0, false, "nothing left to delete");
        assert_eq!(hit(&dev, "a"), serde_json::json!({ "hit": false }));

        dev.get("/put-in-object?k=b").unwrap();
        assert_eq!(hit(&dev, "b")["hit"], true);
    }
    let dev = Dev::start(dir.path());
    assert_eq!(hit(&dev, "b"), serde_json::json!({ "hit": false }), "a node started again has an empty cache");
}

// Goal: what a put keeps follows Cloudflare's rules: a lifetime from
// Cache-Control, or a status's default; nothing for no-store, private,
// no-cache, max-age=0, a Set-Cookie, or a status with no default; a
// private=Set-Cookie response kept without its cookie; a put that keeps
// nothing replaces what the URL held, but a 304 replaces nothing. A match
// or delete of a non-GET request considers it only with ignoreMethod; a
// range or conditional request misses. The key is the URL less its
// fragment.
#[test]
fn a_put_keeps_what_cloudflare_keeps() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path());
    let dev = Dev::start(dir.path());
    for (query, kept) in [
        ("k=cc&cc=max-age%3D60", true),
        ("k=default200", true),
        ("k=d404&status=404", true),
        ("k=d500&status=500", false),
        ("k=cc500&status=500&cc=max-age%3D60", true),
        ("k=nostore&cc=no-store%2C%20max-age%3D60", false),
        ("k=private&cc=private%2C%20max-age%3D60", false),
        ("k=nocache&cc=no-cache", false),
        ("k=zero&cc=max-age%3D0", false),
        ("k=cookie&cookie&cc=max-age%3D60", false),
        ("k=empty&status=204&cc=max-age%3D60", true),
    ] {
        let (status, _) = dev.get(&format!("/put?{query}")).unwrap();
        assert_eq!(status, 200, "{query}");
        let k = query.split('&').next().unwrap().trim_start_matches("k=");
        assert_eq!(hit(&dev, k)["hit"], kept, "{query}: {}", dev.output());
    }
    assert_eq!(hit(&dev, "empty")["status"], 204);
    assert_eq!(hit(&dev, "d404")["status"], 404);

    dev.get("/put?k=kept-cookie&cookie&mine&cc=private%3D%22Set-Cookie%2C%20X-Mine%22%2C%20max-age%3D60").unwrap();
    let got = hit(&dev, "kept-cookie");
    assert_eq!((got["hit"].as_bool(), got["cookie"].as_str(), got["mine"].as_str()), (Some(true), None, None), "kept without the fields named: {got}");

    dev.get("/put?k=cc&cc=no-store").unwrap();
    assert_eq!(hit(&dev, "cc")["hit"], false, "a put that keeps nothing replaces what the URL held");

    dev.get("/put?k=post&cc=max-age%3D60").unwrap();
    let got = dev.json("/match-post?k=post").0;
    assert_eq!((got["plain"]["hit"].as_bool(), got["ignoring"]["hit"].as_bool()), (Some(false), Some(true)), "{got}");

    // a 304 validates what the URL holds and replaces nothing
    dev.get("/put?k=validated&cc=max-age%3D60").unwrap();
    dev.get("/put?k=validated&status=304&cc=max-age%3D60").unwrap();
    assert_eq!((hit(&dev, "validated")["status"].as_u64(), hit(&dev, "validated")["body"].as_str()), (Some(200), Some("body of validated")));

    // a range or a conditional request misses (Cloudflare's 206 and 304 are not answered); the plain one hits
    let got = dev.json("/match-partial?k=validated").0;
    assert_eq!(
        [&got["range"]["hit"], &got["etag"]["hit"], &got["since"]["hit"], &got["whole"]["hit"]],
        [false, false, false, true],
        "{got}"
    );

    // the URL is the key as written, less its fragment: a bare `?` is another URL
    dev.get("/put?k=report%3F&cc=max-age%3D60").unwrap();
    assert_eq!((hit(&dev, "report")["hit"].as_bool(), hit(&dev, "report%3F")["hit"].as_bool()), (Some(false), Some(true)));
    dev.get("/put?k=frag%23one&cc=max-age%3D60").unwrap();
    assert_eq!(hit(&dev, "frag%23two")["hit"], true, "the fragment is not part of it");
}

// Goal: a named cache keeps its own entries apart from the default's, and
// a Dynamic Worker's cache keeps nothing: neither its own put nor the
// platform's entries are matched there.
#[test]
fn named_caches_and_dynamic_workers_keep_their_own() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path());
    let dev = Dev::start(dir.path());
    let got = dev.json("/named?k=n").0;
    assert_eq!((got["named"]["hit"].as_bool(), got["default"]["hit"].as_bool()), (Some(true), Some(false)), "{got}");

    dev.get("/put?k=a&cc=max-age%3D60").unwrap();
    assert_eq!(hit(&dev, "a")["hit"], true);
    for _ in 0..2 {
        assert_eq!(dev.json("/loaded").0, serde_json::json!({ "own": false, "platforms": false }), "{}", dev.output());
    }
}
