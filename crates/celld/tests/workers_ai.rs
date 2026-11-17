//! A Worker calls Workers AI's Whisper on celld as it calls it on
//! Cloudflare (`env.AI.run(model, inputs, options)`), and the node's
//! operator's whisper.cpp server answers it (ai.rs, `CELLD_AI_FILE`): the
//! catalog's input as whisper.cpp's form, its `verbose_json` as the
//! catalog's answer, raw (`returnRawResponse`) or as an object, with the
//! options fragment's cell passes (an AI Gateway's, which a node takes and
//! has no use for). Another model, an input out of shape, and a server that
//! is down each answer as Workers AI would, saying why. The server here is
//! a fake that answers what whisper.cpp answers.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::path::Path;
use std::sync::{Arc, Mutex};
use support::Dev;

const WORKER: &str = r#"
const WHISPER = "@cf/openai/whisper-large-v3-turbo";
// fragment's cell's options (cell/src/models.rs, `workers_ai`)
const OPTIONS = { gateway: { id: "node", metadata: { user_id: "u" }, collectLog: false }, extraHeaders: { "x-session-affinity": "a" } };
const settle = async (f) => {
  try {
    return { value: await f() };
  } catch (e) {
    return { name: e?.name ?? null, code: e?.code ?? null, message: String(e?.message ?? e) };
  }
};
const raw = async (inputs) => {
  const r = await env_.AI.run(WHISPER, inputs, { ...OPTIONS, returnRawResponse: true });
  return { status: r.status, type: r.headers.get("content-type"), body: await r.json() };
};
let env_;
export default {
  async fetch(request, env) {
    env_ = env;
    const audio = btoa("RIFF fake wav");
    switch (new URL(request.url).pathname) {
      case "/raw":
        return Response.json(await settle(() => raw({ audio, task: "transcribe", vad_filter: true })));
      case "/raw-de":
        return Response.json(await settle(() => raw({ audio, task: "transcribe", vad_filter: true, language: "de", initial_prompt: "Paul" })));
      case "/object":
        return Response.json(await settle(() => env.AI.run(WHISPER, { audio })));
      case "/refused":
        return Response.json({
          raw: await settle(() => raw({ audio, prefix: "so" })),
          object: await settle(() => env.AI.run(WHISPER, { audio: "" })),
          other: await settle(() => env.AI.run("@cf/meta/llama-3.1-8b-instruct", { prompt: "x" })),
          queued: await settle(() => env.AI.run(WHISPER, { audio }, { queueRequest: true })),
        });
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
  "ai": { "binding": "AI" }
}"#;

/// A whisper.cpp server's stand-in: each request's form, kept, answered
/// with `verbose_json` as whisper.cpp's server writes it.
fn fake_whisper_cpp() -> (String, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let kept = seen.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 65536];
            let body_at = loop {
                let n = stream.read(&mut chunk).unwrap_or(0);
                if n == 0 {
                    break None;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break Some(at + 4);
                }
            };
            let Some(body_at) = body_at else { continue };
            let head = String::from_utf8_lossy(&buf[..body_at]).to_lowercase();
            let length: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
            while buf.len() < body_at + length {
                let n = stream.read(&mut chunk).unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let form = String::from_utf8_lossy(&buf[body_at..]).to_string();
            let german = form.contains("name=\"language\"\r\n\r\nde\r\n");
            kept.lock().unwrap().push(format!("{}\n{form}", head.lines().next().unwrap_or("")));
            let answer = serde_json::json!({
                "task": "transcribe",
                "language": if german { "german" } else { "english" },
                "duration": 2.5,
                "text": " Hello from a voice memo.\n Second line.\n",
                "segments": [
                    { "id": 0, "text": " Hello from a voice memo.", "start": 0.0, "end": 1.5, "tokens": [1], "temperature": 0.0, "avg_logprob": -0.1, "no_speech_prob": 0.01 },
                    { "id": 1, "text": " Second line.", "start": 1.5, "end": 2.4, "tokens": [2], "temperature": 0.0, "avg_logprob": -0.2, "no_speech_prob": 0.02 }
                ]
            })
            .to_string();
            let _ = write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}", answer.len());
        }
    });
    (origin, seen)
}

fn start(dir: &Path, origin: &str) -> Dev {
    std::fs::write(dir.join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(dir.join("index.js"), WORKER).unwrap();
    let file = dir.join("ai.json");
    std::fs::write(
        &file,
        serde_json::json!({ "models": { "@cf/openai/whisper-large-v3-turbo": { "engine": "whisper.cpp", "origin": origin } } }).to_string(),
    )
    .unwrap();
    let file = file.display().to_string();
    Dev::start_with(dir, &[("CELLD_AI_FILE", file.as_str())])
}

// Goal: a Worker's Whisper call, with fragment's options, is whisper.cpp's
// form and its answer the catalog's, raw or as an object. Replay: asked
// twice, the same answers.
#[test]
fn whisper_is_answered_by_the_nodes_whisper_cpp() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, seen) = fake_whisper_cpp();
    let dev = start(dir.path(), &origin);
    for _ in 0..2 {
        let (got, _) = dev.json("/raw");
        let r = &got["value"];
        assert_eq!(r["status"], 200, "{got}\n{}", dev.output());
        assert_eq!(r["type"], "application/json");
        assert_eq!(r["body"]["text"], "Hello from a voice memo. Second line.", "{got}");
        assert_eq!(r["body"]["word_count"], 7);
        assert_eq!(r["body"]["transcription_info"], serde_json::json!({ "language": "en", "duration": 2.5 }));
        assert_eq!(r["body"]["segments"][1]["text"], "Second line.");
        assert!(r["body"]["vtt"].as_str().unwrap().starts_with("WEBVTT\n\n00:00:00.000 --> 00:00:01.500\nHello from a voice memo."));
        let form = seen.lock().unwrap().last().cloned().unwrap();
        assert!(form.starts_with("post /inference http/1.1"), "{form}");
        for field in [("language", "auto"), ("vad", "true"), ("response_format", "verbose_json"), ("temperature", "0")] {
            assert!(form.contains(&format!("name=\"{}\"\r\n\r\n{}\r\n", field.0, field.1)), "{field:?} in {form}");
        }
        assert!(form.contains("filename=\"audio\"") && form.contains("RIFF fake wav"), "{form}");
        assert!(!form.contains("x-session-affinity") && !form.contains("user_id"), "the gateway's options go nowhere");
    }
    let (got, _) = dev.json("/raw-de");
    assert_eq!(got["value"]["body"]["transcription_info"]["language"], "de", "{got}");
    let form = seen.lock().unwrap().last().cloned().unwrap();
    assert!(form.contains("name=\"language\"\r\n\r\nde\r\n") && form.contains("name=\"prompt\"\r\n\r\nPaul\r\n"), "{form}");
    let (got, _) = dev.json("/object");
    assert_eq!(got["value"]["text"], "Hello from a voice memo. Second line.", "{got}");
    assert!(!dev.output().contains("Hello from a voice memo"), "no words in the node's log");
}

// Invalid: an input out of shape is Workers AI's 400 (raw) or a rejection
// (object); another model, and an option that changes what a call does,
// reject saying why; a server that is down rejects naming it; nothing is
// sent to the server for any of them.
#[test]
fn whisper_out_of_shape_or_unserved_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, seen) = fake_whisper_cpp();
    let dev = start(dir.path(), &origin);
    for _ in 0..2 {
        let (got, _) = dev.json("/refused");
        assert_eq!(got["raw"]["value"]["status"], 400, "{got}");
        assert!(got["raw"]["value"]["body"]["errors"][0]["message"].as_str().unwrap().contains("prefix"), "{got}");
        assert_eq!(got["object"]["name"], "InferenceUpstreamError", "{got}");
        assert!(got["object"]["message"].as_str().unwrap().contains("audio is empty"), "{got}");
        assert!(got["other"]["message"].as_str().unwrap().starts_with("this node has no Workers AI model @cf/meta/llama-3.1-8b-instruct"), "{got}");
        assert!(got["queued"]["message"].as_str().unwrap().contains("queueRequest"), "{got}");
    }
    assert!(seen.lock().unwrap().is_empty(), "nothing reached the server");
    drop(dev);

    // the server is down: the call rejects, naming where it went
    let other = tempfile::tempdir().unwrap();
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let dev = start(other.path(), &origin);
    let (got, _) = dev.json("/object");
    assert_eq!(got["code"], "down", "{got}");
    assert!(got["message"].as_str().unwrap().contains(&format!("whisper.cpp at {origin} is not answering")), "{got}");
}

// A node's AI file that names what celld does not run stops the start,
// saying what it takes.
#[test]
fn an_ai_file_out_of_shape_stops_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("ai.json");
    std::fs::write(&file, r#"{"models":{"@cf/meta/llama-3.1-8b-instruct":{"engine":"whisper.cpp","origin":"http://127.0.0.1:1"}}}"#).unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_celld"))
        .args(["--no-control-plane", "--listen", "127.0.0.1:0"])
        .env("CELLD_AI_FILE", &file)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(said.contains("which celld does not translate"), "{said}");
}
