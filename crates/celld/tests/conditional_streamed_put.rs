//! A conditional R2 `put` of a streamed body is applied as R2 applies it,
//! whatever its size (R2 takes `onlyIf` on a put of up to 5 GiB in one
//! request): a body past the multipart part size is written once, if
//! absent, and refused (`null`) once the name is taken. celld holds such a
//! body whole, up to its bound, and refuses one past it, saying so. An
//! unconditional body that size is still written as a multipart upload.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use support::{project, Dev};

const WORKER: &str = r#"
const hex = (buf) => [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key");
    const sha256 = url.searchParams.get("sha256") || undefined;
    const conditional = url.searchParams.get("if") === "absent";
    const options = conditional ? { onlyIf: new Headers({ "if-none-match": "*" }), sha256 } : { sha256 };
    try {
      const put = await env.BLOBS.put(key, request.body, options);
      const head = await env.BLOBS.head(key);
      return Response.json({ stored: put !== null, size: head?.size ?? null, sha256: head?.checksums?.sha256 ? hex(head.checksums.sha256) : null });
    } catch (e) {
      return Response.json({ error: String((e && e.message) || e) }, { status: 500 });
    }
  },
};
"#;

/// Past the 8 MiB part a streamed put cuts.
const BIG: usize = 20 << 20;
/// Past the 256 MiB a conditional put holds.
const TOO_BIG: usize = (256 << 20) + (1 << 20);

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn a_conditional_put_of_a_big_stream_is_applied_as_r2_applies_it() {
    let dir = tempfile::tempdir().unwrap();
    project(dir.path(), WORKER, &[]);
    let config = dir.path().join("wrangler.jsonc");
    let with_bucket = std::fs::read_to_string(&config)
        .unwrap()
        .replace(r#""durable_objects""#, r#""r2_buckets": [{ "binding": "BLOBS", "bucket_name": "blobs" }],
  "durable_objects""#);
    std::fs::write(&config, with_bucket).unwrap();
    let dev = Dev::start(dir.path());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let client = reqwest::Client::new();
    // a body streamed in 1 MiB chunks, so it reaches the Worker as a stream
    let put = |query: &str, size: usize, byte: u8| {
        let url = format!("http://127.0.0.1:{}/?{query}", dev.port);
        let chunks = (0..size.div_ceil(1 << 20)).map(move |i| Ok::<_, std::io::Error>(vec![byte; (size - i * (1 << 20)).min(1 << 20)]));
        runtime.block_on(async {
            let response = client.put(url).body(reqwest::Body::wrap_stream(futures_util::stream::iter(chunks))).send().await.unwrap();
            (response.status().as_u16(), response.json::<serde_json::Value>().await.unwrap())
        })
    };
    let big = vec![b'a'; BIG];
    let sum = sha256_hex(&big);

    // valid: created, whole, its checksum kept
    let (status, first) = put(&format!("key=big&if=absent&sha256={sum}"), BIG, b'a');
    assert_eq!((status, &first["stored"], &first["size"], &first["sha256"]), (200, &serde_json::json!(true), &serde_json::json!(BIG), &serde_json::json!(sum)), "{first}\n{}", dev.output());
    // replay: the name is taken, so the same put is refused, nothing changed
    let (status, again) = put(&format!("key=big&if=absent&sha256={sum}"), BIG, b'a');
    assert_eq!((status, &again["stored"], &again["size"]), (200, &serde_json::json!(false), &serde_json::json!(BIG)), "{again}");
    // invalid: a body that is not its checksum is not written
    let (status, wrong) = put(&format!("key=wrong&if=absent&sha256={sum}"), BIG, b'b');
    assert_eq!(status, 500, "{wrong}");
    assert!(wrong["error"].as_str().unwrap_or_default().contains("sha256"), "{wrong}");
    // past the bound: refused, saying why
    let (status, past) = put("key=past&if=absent", TOO_BIG, b'a');
    assert_eq!(status, 500, "{past}");
    assert!(past["error"].as_str().unwrap_or_default().contains("holds its body"), "{past}");
    // a small conditional put, and a big unconditional one, as before
    let (status, small) = put("key=small&if=absent", 1024, b'a');
    assert_eq!((status, &small["stored"], &small["size"]), (200, &serde_json::json!(true), &serde_json::json!(1024)), "{small}");
    let (status, plain) = put("key=plain", BIG, b'a');
    assert_eq!((status, &plain["stored"], &plain["size"]), (200, &serde_json::json!(true), &serde_json::json!(BIG)), "{plain}");
}
