//! A Worker calls Workers AI on celld as it calls it on Cloudflare
//! (`env.AI.run(model, inputs, options)`), and the endpoints the node's
//! operator names answer it (ai.rs, `CELLD_AI_FILE`): Whisper on a
//! whisper.cpp server or an OpenAI-compatible one, the catalog's input as
//! its form and its `verbose_json` as the catalog's answer, raw
//! (`returnRawResponse`) or as an object, with the options fragment's cell
//! passes (an AI Gateway's, which a node takes and has no use for); and
//! every other model on the `"*"` endpoint, its answer passed on as it
//! came. A model with no endpoint, an input out of shape, and an endpoint
//! that is down each answer as Workers AI would, saying why. The endpoint
//! here is a fake that answers as each server does.

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
export default {
  async fetch(request, env) {
    const raw = async (model, inputs) => {
      const r = await env.AI.run(model, inputs, { ...OPTIONS, returnRawResponse: true });
      const type = r.headers.get("content-type");
      return { status: r.status, type, log: r.headers.get("cf-aig-log-id"), body: type.includes("json") ? await r.json() : (await r.arrayBuffer()).byteLength };
    };
    const audio = btoa("RIFF fake wav");
    switch (new URL(request.url).pathname) {
      case "/raw":
        return Response.json(await settle(() => raw(WHISPER, { audio, task: "transcribe", vad_filter: true })));
      case "/raw-de":
        return Response.json(await settle(() => raw(WHISPER, { audio, task: "transcribe", vad_filter: true, language: "de", initial_prompt: "Paul" })));
      case "/object":
        return Response.json(await settle(() => env.AI.run(WHISPER, { audio })));
      case "/passed":
        return Response.json({
          raw: await settle(() => raw("@cf/meta/llama-3.1-8b-instruct", { messages: [{ role: "user", content: "hi" }] })),
          object: await settle(() => env.AI.run("@cf/meta/llama-3.1-8b-instruct", { prompt: "hi" })),
          image: await settle(() => raw("@cf/x/png-model", { prompt: "a cat" })),
          stream: await settle(async () => (await new Response(await env.AI.run("@cf/x/png-model", { prompt: "a cat" })).arrayBuffer()).byteLength),
          refused: await settle(() => raw("@cf/x/refuses", { prompt: "x" })),
        });
      case "/refused":
        return Response.json({
          raw: await settle(() => raw(WHISPER, { audio, prefix: "so" })),
          object: await settle(() => env.AI.run(WHISPER, { audio: "" })),
          other: await settle(() => env.AI.run("@cf/meta/llama-3.1-8b-instruct", { prompt: "x" })),
          queued: await settle(() => env.AI.run(WHISPER, { audio }, { queueRequest: true })),
          form: await settle(() => env.AI.run("@cf/black-forest-labs/flux-2-dev", { multipart: { body: new Response("x").body, contentType: "multipart/form-data; boundary=b" } })),
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

/// One request the fake heard: its request line and its body.
type Heard = Arc<Mutex<Vec<String>>>;

/// An endpoint's stand-in: each request kept, answered as whisper.cpp's
/// server (`/inference`), an OpenAI-compatible one
/// (`/v1/audio/transcriptions`) or a Workers AI endpoint (`/run/<model>`)
/// answers.
fn fake_endpoint() -> (String, Heard) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let seen: Heard = Arc::new(Mutex::new(Vec::new()));
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
            let length: usize = head.lines().find_map(|l| l.strip_prefix("content-length:")).and_then(|v| v.trim().parse().ok()).unwrap_or(0);
            while buf.len() < body_at + length {
                let n = stream.read(&mut chunk).unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            let line = head.lines().next().unwrap_or("").to_string();
            let body = String::from_utf8_lossy(&buf[body_at..]).to_string();
            kept.lock().unwrap().push(format!("{line}\n{body}"));
            let german = body.contains("name=\"language\"\r\n\r\nde\r\n");
            let segments = serde_json::json!([
                { "id": 0, "text": " Hello from a voice memo.", "start": 0.0, "end": 1.5, "tokens": [1], "temperature": 0.0, "avg_logprob": -0.1, "no_speech_prob": 0.01 },
                { "id": 1, "text": " Second line.", "start": 1.5, "end": 2.4, "tokens": [2], "temperature": 0.0, "avg_logprob": -0.2, "no_speech_prob": 0.02 }
            ]);
            let (status, kind, extra, answer): (&str, &str, &str, Vec<u8>) = if line.starts_with("post /inference ") {
                let lang = if german { "german" } else { "english" };
                ("200 OK", "application/json", "", serde_json::json!({ "task": "transcribe", "language": lang, "duration": 2.5, "text": " Hello from a voice memo.\n Second line.\n", "segments": segments }).to_string().into_bytes())
            } else if line.starts_with("post /v1/audio/transcriptions ") {
                ("200 OK", "application/json", "", serde_json::json!({ "task": "transcribe", "language": "English", "duration": 2.5, "text": " Hello from a voice memo. Second line.", "segments": segments }).to_string().into_bytes())
            } else if line.starts_with("post /run/@cf/x/png-model ") {
                ("200 OK", "image/png", "cf-ai-neurons: 7\r\n", b"\x89PNG\r\n\x1a\nfake".to_vec())
            } else if line.starts_with("post /run/@cf/x/refuses ") {
                ("400 Bad Request", "application/json", "", br#"{"errors":[{"code":5006,"message":"no"}],"success":false}"#.to_vec())
            } else if let Some(model) = line.strip_prefix("post /run/") {
                let model = model.split(' ').next().unwrap_or("");
                ("200 OK", "application/json", "cf-aig-log-id: log-1\r\n", serde_json::json!({ "response": "hello", "model": model, "heard": serde_json::from_str::<serde_json::Value>(&body).unwrap_or_default() }).to_string().into_bytes())
            } else {
                ("404 Not Found", "text/plain", "", b"where?".to_vec())
            };
            let _ = write!(stream, "HTTP/1.1 {status}\r\ncontent-type: {kind}\r\n{extra}content-length: {}\r\nconnection: close\r\n\r\n", answer.len());
            let _ = stream.write_all(&answer);
        }
    });
    (origin, seen)
}

fn start(dir: &Path, models: serde_json::Value) -> Dev {
    std::fs::write(dir.join("wrangler.jsonc"), CONFIG).unwrap();
    std::fs::write(dir.join("index.js"), WORKER).unwrap();
    let file = dir.join("ai.json");
    std::fs::write(&file, serde_json::json!({ "models": models }).to_string()).unwrap();
    let file = file.display().to_string();
    Dev::start_with(dir, &[("CELLD_AI_FILE", file.as_str())])
}

fn last(seen: &Heard) -> String {
    seen.lock().unwrap().last().cloned().unwrap_or_default()
}

// Goal: a Worker's Whisper call, with fragment's options, is whisper.cpp's
// form and its answer the catalog's, raw or as an object; every other model
// goes to the "*" endpoint and comes back as it came, JSON or bytes, a
// refusal by its status. Replay: asked twice, the same answers.
#[test]
fn whisper_is_the_nodes_and_the_rest_pass_on() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, seen) = fake_endpoint();
    let dev = start(
        dir.path(),
        serde_json::json!({
            "@cf/openai/whisper-large-v3-turbo": { "engine": "whisper.cpp", "origin": origin },
            "*": { "engine": "workers-ai", "origin": origin }
        }),
    );
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
        let form = last(&seen);
        assert!(form.starts_with("post /inference http/1.1"), "{form}");
        for field in [("language", "auto"), ("vad", "true"), ("response_format", "verbose_json"), ("temperature", "0")] {
            assert!(form.contains(&format!("name=\"{}\"\r\n\r\n{}\r\n", field.0, field.1)), "{field:?} in {form}");
        }
        assert!(form.contains("filename=\"audio\"") && form.contains("RIFF fake wav"), "{form}");
        assert!(!form.contains("x-session-affinity") && !form.contains("user_id"), "the gateway's options go nowhere");

        let (got, _) = dev.json("/passed");
        assert_eq!(got["raw"]["value"]["status"], 200, "{got}\n{}", dev.output());
        assert_eq!(got["raw"]["value"]["log"], "log-1", "the binding's headers are passed on");
        assert_eq!(got["raw"]["value"]["body"]["model"], "@cf/meta/llama-3.1-8b-instruct");
        assert_eq!(got["raw"]["value"]["body"]["heard"], serde_json::json!({ "messages": [{ "role": "user", "content": "hi" }] }), "the input as it was");
        assert_eq!(got["object"]["value"]["response"], "hello", "{got}");
        assert_eq!((got["image"]["value"]["type"].clone(), got["image"]["value"]["body"].clone()), (serde_json::json!("image/png"), serde_json::json!(12)), "{got}");
        assert_eq!(got["stream"]["value"], 12, "bytes, without returnRawResponse, are its body: {got}");
        assert_eq!((got["refused"]["value"]["status"].clone(), got["refused"]["value"]["body"]["errors"][0]["message"].clone()), (serde_json::json!(400), serde_json::json!("no")), "{got}");
    }
    let (got, _) = dev.json("/raw-de");
    assert_eq!(got["value"]["body"]["transcription_info"]["language"], "de", "{got}");
    let form = last(&seen);
    assert!(form.contains("name=\"language\"\r\n\r\nde\r\n") && form.contains("name=\"prompt\"\r\n\r\nPaul\r\n"), "{form}");
    let (got, _) = dev.json("/object");
    assert_eq!(got["value"]["text"], "Hello from a voice memo. Second line.", "{got}");
    assert!(!dev.output().contains("Hello from a voice memo"), "no words in the node's log");
}

// Valid: Whisper on an OpenAI-compatible server is its
// /audio/transcriptions form, the server's model named, no `auto`.
#[test]
fn whisper_on_an_openai_compatible_server() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, seen) = fake_endpoint();
    let dev = start(dir.path(), serde_json::json!({ "@cf/openai/whisper-large-v3-turbo": { "engine": "openai", "base_url": format!("{origin}/v1"), "model": "Systran/faster-whisper-small" } }));
    let (got, _) = dev.json("/raw");
    assert_eq!(got["value"]["status"], 200, "{got}\n{}", dev.output());
    assert_eq!(got["value"]["body"]["text"], "Hello from a voice memo. Second line.");
    assert_eq!(got["value"]["body"]["transcription_info"], serde_json::json!({ "language": "en", "duration": 2.5 }));
    let form = last(&seen);
    assert!(form.starts_with("post /v1/audio/transcriptions http/1.1"), "{form}");
    assert!(form.contains("name=\"model\"\r\n\r\nSystran/faster-whisper-small\r\n") && form.contains("name=\"vad_filter\"\r\n\r\ntrue\r\n"), "{form}");
    assert!(!form.contains("name=\"language\""), "OpenAI's API detects a language it is not told: {form}");
}

// Invalid: an input out of shape is Workers AI's 400 (raw) or a rejection
// (object); a model with no endpoint, an option that changes what a call
// does, and a multipart input reject saying why; an endpoint that is down
// rejects naming it; nothing is sent for any of them.
#[test]
fn out_of_shape_or_unserved_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (origin, seen) = fake_endpoint();
    let dev = start(dir.path(), serde_json::json!({ "@cf/openai/whisper-large-v3-turbo": { "engine": "whisper.cpp", "origin": origin } }));
    for _ in 0..2 {
        let (got, _) = dev.json("/refused");
        assert_eq!(got["raw"]["value"]["status"], 400, "{got}");
        assert!(got["raw"]["value"]["body"]["errors"][0]["message"].as_str().unwrap().contains("prefix"), "{got}");
        assert_eq!(got["object"]["name"], "InferenceUpstreamError", "{got}");
        assert!(got["object"]["message"].as_str().unwrap().contains("audio is empty"), "{got}");
        assert!(got["other"]["message"].as_str().unwrap().starts_with("this node has no Workers AI model @cf/meta/llama-3.1-8b-instruct"), "{got}");
        assert_eq!(got["other"]["code"], "unbound", "{got}");
        assert!(got["queued"]["message"].as_str().unwrap().contains("queueRequest"), "{got}");
        assert!(got["form"]["message"].as_str().unwrap().contains("multipart"), "{got}");
    }
    assert!(seen.lock().unwrap().is_empty(), "nothing reached the endpoint");
    drop(dev);

    // the endpoint is down: the call rejects, naming where it went
    let other = tempfile::tempdir().unwrap();
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let dev = start(other.path(), serde_json::json!({ "@cf/openai/whisper-large-v3-turbo": { "engine": "whisper.cpp", "origin": origin } }));
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
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_celld")).args(["--no-control-plane", "--listen", "127.0.0.1:0"]).env("CELLD_AI_FILE", &file).output().unwrap();
    assert!(!out.status.success());
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(said.contains("a transcription engine serves @cf/openai/whisper-large-v3-turbo"), "{said}");
}
