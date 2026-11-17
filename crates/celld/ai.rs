//! fork: a node's Workers AI, the `ai` binding's `run()` (harness.js, `Ai`;
//! docs/services/workers-ai.md). Cloudflare's binding runs a model of its
//! catalog by its id; celld runs only the models the node's operator names,
//! each on a server the operator runs beside the node, and answers in the
//! catalog's shapes, so a Worker written for Cloudflare calls it as it is.
//! Every other model rejects, saying the node has none.
//!
//! `CELLD_AI_FILE` names a JSON file that maps a catalog id to its server:
//!
//! ```json
//! { "models": { "@cf/openai/whisper-large-v3-turbo":
//!     { "engine": "whisper.cpp", "origin": "http://127.0.0.1:8178", "concurrency": 4 } } }
//! ```
//!
//! One engine so far: `whisper.cpp`'s server (`/inference`, started with
//! `--convert`, so it reads what browsers and phones record, and with a VAD
//! model, for `vad_filter`), for the catalog's Whisper (`TRANSCRIBE`). The
//! server is the node's own service, usually on loopback, which a Worker's
//! fetch never reaches: the host calls it, outside every isolate, and the
//! Worker never sees its address. Its `/load` swaps the model, so only the
//! node may reach it.
//!
//! A call is bounded (`AUDIO_MAX_BYTES`, `ANSWER_MAX_BYTES`, `CALL_TIMEOUT`)
//! and at most `concurrency` are in flight to one server (the server runs
//! one at a time and queues the rest); past that the call is answered 429,
//! as Workers AI answers one it has no capacity for, and its caller tries
//! again. Nothing is logged of the audio or the words.

use anyhow::{anyhow, bail};
use base64::Engine as _;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// The variable that names the node's AI file.
pub const VARIABLE: &str = "CELLD_AI_FILE";
/// The AI file, at most.
pub const FILE_MAX_BYTES: u64 = 64 * 1024;
/// The catalog's Whisper (developers.cloudflare.com/workers-ai/models/
/// whisper-large-v3-turbo/): `{audio: <base64>, task?, language?,
/// vad_filter?, initial_prompt?, …}` in, `{text, word_count, segments, vtt,
/// transcription_info: {language, duration, …}}` out.
pub const TRANSCRIBE: &str = "@cf/openai/whisper-large-v3-turbo";
/// The audio of one call, at most (OpenAI's own transcription limit).
pub const AUDIO_MAX_BYTES: usize = 25 * 1024 * 1024;
/// A call's inputs as the isolate sends them: the audio as base64, and room.
pub const INPUT_MAX_BYTES: usize = AUDIO_MAX_BYTES / 3 * 4 + 64 * 1024;
/// The server's answer, at most: an hour's segments and their tokens.
pub const ANSWER_MAX_BYTES: usize = 16 * 1024 * 1024;
/// One call, at most, the server's queue included.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(600);
/// How many calls a server has in flight, by default.
pub const CONCURRENCY_DEFAULT: usize = 4;
/// And at most.
pub const CONCURRENCY_MAX: usize = 64;
/// A prompt, at most (Whisper reads 224 tokens of one).
const PROMPT_MAX_BYTES: usize = 4096;

/// Why a call is not answered: the binding rejects with `message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub code: &'static str,
    pub message: String,
}

fn failure(code: &'static str, message: impl Into<String>) -> Failure {
    Failure { code, message: message.into() }
}

/// An answer: the status and JSON body Workers AI's REST API would give.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub status: u16,
    pub body: Value,
}

/// A refusal in Workers AI's shape (`{errors: [{code, message}], success}`).
fn refused(status: u16, code: u32, message: impl Into<String>) -> Answer {
    Answer { status, body: json!({ "success": false, "errors": [{ "code": code, "message": message.into() }] }) }
}

/// One model's server.
#[derive(Debug)]
pub struct Server {
    origin: String,
    slots: Arc<tokio::sync::Semaphore>,
}

/// The node's models, by catalog id.
#[derive(Debug, Default)]
pub struct Models {
    servers: BTreeMap<String, Server>,
    file: PathBuf,
}

impl Models {
    /// The models `text` (an AI file's contents, read from `file`) names, or
    /// why it is refused.
    pub fn parse(text: &[u8], file: &Path) -> anyhow::Result<Models> {
        let parsed: Value = serde_json::from_slice(text).map_err(|e| anyhow!("{VARIABLE} {} is not JSON: {e}", file.display()))?;
        let Some(top) = parsed.as_object() else {
            bail!("{VARIABLE} {} holds an object: {{\"models\": {{…}}}}", file.display());
        };
        if let Some(key) = top.keys().find(|k| *k != "models") {
            bail!("{VARIABLE} takes `models`, not `{key}`");
        }
        let Some(Value::Object(models)) = top.get("models") else {
            bail!("{VARIABLE} names its `models`: an object of each catalog id and its server");
        };
        let mut servers = BTreeMap::new();
        for (id, entry) in models {
            if id != TRANSCRIBE {
                bail!("{VARIABLE} names {id:?}, which celld does not translate; it runs {TRANSCRIBE}");
            }
            let Value::Object(entry) = entry else {
                bail!("{VARIABLE}'s {id} is an object: {{\"engine\", \"origin\"}}");
            };
            if let Some(key) = entry.keys().find(|k| !["engine", "origin", "concurrency"].contains(&k.as_str())) {
                bail!("{VARIABLE}'s {id} takes engine, origin and concurrency, not `{key}`");
            }
            if entry.get("engine").and_then(Value::as_str) != Some("whisper.cpp") {
                bail!("{VARIABLE}'s {id} names its engine: \"whisper.cpp\" (its server)");
            }
            let origin = entry.get("origin").and_then(Value::as_str).unwrap_or_default();
            if !crate::secrets::origin(origin) {
                bail!("{VARIABLE}'s {id} names its server's origin (http://127.0.0.1:8178, no path)");
            }
            let concurrency = match entry.get("concurrency") {
                None => CONCURRENCY_DEFAULT,
                Some(n) => match n.as_u64() {
                    Some(n) if (1..=CONCURRENCY_MAX as u64).contains(&n) => n as usize,
                    _ => bail!("{VARIABLE}'s {id} concurrency is 1 to {CONCURRENCY_MAX}"),
                },
            };
            servers.insert(id.clone(), Server { origin: origin.to_string(), slots: Arc::new(tokio::sync::Semaphore::new(concurrency)) });
        }
        Ok(Models { servers, file: file.to_path_buf() })
    }

    /// `model`'s answer to `inputs` (the isolate's JSON), or why there is
    /// none.
    pub async fn run(&self, model: &str, inputs: &str) -> Result<Answer, Failure> {
        let Some(server) = self.servers.get(model) else {
            return Err(failure("unbound", format!("this node has no Workers AI model {model} ({VARIABLE} {} names none)", self.file.display())));
        };
        let request = match whisper_request(inputs) {
            Ok(request) => request,
            Err(answer) => return Ok(answer),
        };
        let Ok(_slot) = server.slots.clone().try_acquire_owned() else {
            return Ok(refused(429, 3040, "Capacity temporarily exceeded, please try again."));
        };
        whisper_cpp(&server.origin, request).await
    }
}

/// What a Whisper call asks of whisper.cpp's server.
#[derive(Debug, Clone, PartialEq)]
pub struct WhisperRequest {
    pub audio: Vec<u8>,
    /// `language` (`auto`: detect it), `prompt`, `translate`, `vad`,
    /// `beam_size`, `no_speech_thold`, `logprob_thold`, as its form names
    /// them.
    pub fields: Vec<(&'static str, String)>,
    /// The language the call named, to answer it by its code.
    pub language: Option<String>,
}

/// The catalog's Whisper input, as whisper.cpp's form, or the refusal
/// Workers AI would answer (400). A key of the catalog's that celld does
/// not translate is refused by name, never let go: the caller asked for it.
pub fn whisper_request(inputs: &str) -> Result<WhisperRequest, Answer> {
    let bad = |m: String| refused(400, 5006, m);
    if inputs.len() > INPUT_MAX_BYTES {
        return Err(refused(413, 5006, format!("the input is {} bytes; at most {INPUT_MAX_BYTES}", inputs.len())));
    }
    let Ok(Value::Object(input)) = serde_json::from_str::<Value>(inputs) else {
        return Err(bad("the input is an object".into()));
    };
    const TRANSLATED: [&str; 8] = ["audio", "task", "language", "vad_filter", "initial_prompt", "beam_size", "no_speech_threshold", "log_prob_threshold"];
    const NOT_TRANSLATED: [&str; 4] = ["prefix", "condition_on_previous_text", "compression_ratio_threshold", "hallucination_silence_threshold"];
    for key in input.keys() {
        if NOT_TRANSLATED.contains(&key.as_str()) {
            return Err(bad(format!("celld's Whisper does not take `{key}`")));
        }
        if !TRANSLATED.contains(&key.as_str()) {
            return Err(bad(format!("Whisper's input has no `{key}`")));
        }
    }
    let audio = match input.get("audio") {
        Some(Value::String(b64)) => base64::engine::general_purpose::STANDARD.decode(b64).map_err(|_| bad("audio is base64".into()))?,
        Some(_) => return Err(bad("audio is a base64 string".into())),
        None => return Err(bad("the input carries its `audio`".into())),
    };
    if audio.is_empty() {
        return Err(bad("the audio is empty".into()));
    }
    if audio.len() > AUDIO_MAX_BYTES {
        return Err(refused(413, 5006, format!("the audio is {} bytes; at most {AUDIO_MAX_BYTES}", audio.len())));
    }
    let text = |key: &str| -> Result<Option<&str>, Answer> {
        match input.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(Some(s.as_str())),
            Some(_) => Err(bad(format!("{key} is a string"))),
        }
    };
    let number = |key: &str| -> Result<Option<f64>, Answer> {
        match input.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v.as_f64().filter(|n| n.is_finite()).map(Some).ok_or_else(|| bad(format!("{key} is a number"))),
        }
    };
    let mut fields: Vec<(&'static str, String)> = Vec::new();
    let language = match text("language")?.map(str::trim).filter(|l| !l.is_empty()) {
        None => None,
        Some(l) if code_of(l).is_some() => Some(l.to_string()),
        Some(l) => return Err(bad(format!("{l:?} is no language Whisper knows"))),
    };
    fields.push(("language", language.clone().unwrap_or_else(|| "auto".into())));
    match text("task")? {
        None | Some("transcribe") => {}
        Some("translate") => fields.push(("translate", "true".into())),
        Some(_) => return Err(bad("task is transcribe or translate".into())),
    }
    if let Some(prompt) = text("initial_prompt")?.filter(|p| !p.trim().is_empty()) {
        if prompt.len() > PROMPT_MAX_BYTES {
            return Err(bad(format!("initial_prompt is at most {PROMPT_MAX_BYTES} bytes")));
        }
        fields.push(("prompt", prompt.to_string()));
    }
    match input.get("vad_filter") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => {}
        Some(Value::Bool(true)) => fields.push(("vad", "true".into())),
        Some(_) => return Err(bad("vad_filter is true or false".into())),
    }
    if let Some(n) = number("beam_size")? {
        if !(n.fract() == 0.0 && (1.0..=16.0).contains(&n)) {
            return Err(bad("beam_size is 1 to 16".into()));
        }
        fields.push(("beam_size", format!("{n}")));
    }
    if let Some(n) = number("no_speech_threshold")? {
        fields.push(("no_speech_thold", format!("{n}")));
    }
    if let Some(n) = number("log_prob_threshold")? {
        fields.push(("logprob_thold", format!("{n}")));
    }
    fields.push(("response_format", "verbose_json".into()));
    fields.push(("temperature", "0".into()));
    Ok(WhisperRequest { audio, fields, language })
}

/// Whisper's languages, by code and by the name whisper.cpp answers with
/// (its `g_lang`).
const LANGUAGES: &str = "en:english,zh:chinese,de:german,es:spanish,ru:russian,ko:korean,fr:french,ja:japanese,pt:portuguese,tr:turkish,pl:polish,ca:catalan,nl:dutch,ar:arabic,sv:swedish,it:italian,id:indonesian,hi:hindi,fi:finnish,vi:vietnamese,he:hebrew,uk:ukrainian,el:greek,ms:malay,cs:czech,ro:romanian,da:danish,hu:hungarian,ta:tamil,no:norwegian,th:thai,ur:urdu,hr:croatian,bg:bulgarian,lt:lithuanian,la:latin,mi:maori,ml:malayalam,cy:welsh,sk:slovak,te:telugu,fa:persian,lv:latvian,bn:bengali,sr:serbian,az:azerbaijani,sl:slovenian,kn:kannada,et:estonian,mk:macedonian,br:breton,eu:basque,is:icelandic,hy:armenian,ne:nepali,mn:mongolian,bs:bosnian,kk:kazakh,sq:albanian,sw:swahili,gl:galician,mr:marathi,pa:punjabi,si:sinhala,km:khmer,sn:shona,yo:yoruba,so:somali,af:afrikaans,oc:occitan,ka:georgian,be:belarusian,tg:tajik,sd:sindhi,gu:gujarati,am:amharic,yi:yiddish,lo:lao,uz:uzbek,fo:faroese,ht:haitian creole,ps:pashto,tk:turkmen,nn:nynorsk,mt:maltese,sa:sanskrit,lb:luxembourgish,my:myanmar,bo:tibetan,tl:tagalog,mg:malagasy,as:assamese,tt:tatar,haw:hawaiian,ln:lingala,ha:hausa,ba:bashkir,jw:javanese,su:sundanese,yue:cantonese";

/// The code of a language, named by its code or its name.
fn code_of(language: &str) -> Option<&'static str> {
    LANGUAGES.split(',').filter_map(|pair| pair.split_once(':')).find(|(code, name)| *code == language || *name == language).map(|(code, _)| code)
}

/// whisper.cpp's `verbose_json` as the catalog's answer: the text, its
/// words counted, its segments and a WebVTT of them, and what was heard
/// (`transcription_info`: the language's code and the audio's length,
/// which a caller meters by). What whisper.cpp does not measure is left
/// out, never made up: `duration_after_vad`, a segment's
/// `compression_ratio` and its `words`' timings.
pub fn catalog_answer(verbose: &Value, asked: Option<&str>) -> Option<Value> {
    // whisper.cpp ends each segment's text with a newline; each begins with
    // its own space where the language has one
    let text = verbose["text"].as_str()?.replace('\n', "").trim().to_string();
    let duration = verbose["duration"].as_f64().filter(|d| d.is_finite() && *d >= 0.0)?;
    let language = asked.or_else(|| verbose["language"].as_str()).and_then(code_of);
    let mut segments = Vec::new();
    let mut vtt = String::from("WEBVTT\n\n");
    for s in verbose["segments"].as_array().into_iter().flatten() {
        let (start, end) = (s["start"].as_f64().unwrap_or(0.0), s["end"].as_f64().unwrap_or(0.0));
        let said = s["text"].as_str().unwrap_or("").trim();
        let mut segment = json!({ "start": start, "end": end, "text": said });
        for key in ["temperature", "avg_logprob", "no_speech_prob"] {
            if let Some(v) = s.get(key).filter(|v| v.is_number()) {
                segment[key] = v.clone();
            }
        }
        segments.push(segment);
        vtt.push_str(&format!("{} --> {}\n{said}\n\n", stamp(start), stamp(end)));
    }
    let mut info = Map::new();
    if let Some(code) = language {
        info.insert("language".into(), json!(code));
    }
    if let Some(p) = verbose["detected_language_probability"].as_f64().filter(|p| asked.is_none() && p.is_finite()) {
        info.insert("language_probability".into(), json!(p));
    }
    info.insert("duration".into(), json!(duration));
    Some(json!({
        "text": text,
        "word_count": text.split_whitespace().count(),
        "segments": segments,
        "vtt": vtt,
        "transcription_info": info,
    }))
}

/// A WebVTT time, `HH:MM:SS.mmm`.
fn stamp(seconds: f64) -> String {
    let ms = (seconds.max(0.0) * 1000.0).round() as u64;
    format!("{:02}:{:02}:{:02}.{:03}", ms / 3_600_000, ms / 60_000 % 60, ms / 1000 % 60, ms % 1000)
}

/// The client whisper.cpp's server is called with: no proxy, no redirect,
/// each call bounded. Not a Worker's egress: the server is the node's own.
fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .tls_built_in_native_certs(false)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CALL_TIMEOUT)
            .build()
            .expect("build the AI servers' HTTP client")
    })
}

/// `request` as a `multipart/form-data` body, its boundary one the audio
/// does not hold.
fn form(request: &WhisperRequest) -> (String, Vec<u8>) {
    let mut n: u64 = 0x5eed_cafe;
    let boundary = loop {
        let candidate = format!("celld-ai-{n:016x}{:08x}", request.audio.len());
        let b = candidate.as_bytes();
        if !request.audio.windows(b.len()).any(|w| w == b) && request.fields.iter().all(|(_, v)| !v.contains(&candidate)) {
            break candidate;
        }
        n = n.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    };
    let mut body = Vec::with_capacity(request.audio.len() + 4096);
    for (name, value) in &request.fields {
        body.extend_from_slice(format!("--{boundary}\r\ncontent-disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes());
    }
    body.extend_from_slice(format!("--{boundary}\r\ncontent-disposition: form-data; name=\"file\"; filename=\"audio\"\r\ncontent-type: application/octet-stream\r\n\r\n").as_bytes());
    body.extend_from_slice(&request.audio);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (boundary, body)
}

/// whisper.cpp's server at `origin` hears `request`.
async fn whisper_cpp(origin: &str, request: WhisperRequest) -> Result<Answer, Failure> {
    let place = format!("whisper.cpp at {origin}");
    let (boundary, body) = form(&request);
    let sent = client()
        .post(format!("{origin}/inference"))
        .header("content-type", format!("multipart/form-data; boundary={boundary}"))
        .header("accept", "application/json")
        .body(body)
        .send()
        .await;
    let mut response = match sent {
        Ok(response) => response,
        Err(e) if e.is_timeout() => return Err(failure("down", format!("{place} did not answer within {} s", CALL_TIMEOUT.as_secs()))),
        Err(e) => return Err(failure("down", format!("{place} is not answering ({})", e.without_url()))),
    };
    let status = response.status().as_u16();
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if bytes.len() + chunk.len() > ANSWER_MAX_BYTES {
                    return Err(failure("failed", format!("{place} answered {status} with over {ANSWER_MAX_BYTES} bytes")));
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) if e.is_timeout() => return Err(failure("down", format!("{place} did not finish its answer within {} s", CALL_TIMEOUT.as_secs()))),
            Err(e) => return Err(failure("down", format!("{place} broke off its answer ({})", e.without_url()))),
        }
    }
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    let said: String = body["error"].as_str().unwrap_or("").chars().take(200).collect();
    Ok(match status {
        200 => match catalog_answer(&body, request.language.as_deref()) {
            Some(answer) => Answer { status: 200, body: answer },
            None => refused(502, 3000, format!("{place} answered with no text or length")),
        },
        // the audio is none ffmpeg reads: the call's, as Workers AI's 400
        500 if said.contains("FFmpeg conversion failed") => refused(400, 5006, "the audio could not be read"),
        400 => refused(400, 5006, format!("{place} refused the call: {said}")),
        // loading its model
        503 => refused(503, 3040, format!("{place} is not ready")),
        _ => refused(502, 3000, format!("{place} answered {status}: {said}")),
    })
}

/// The node's models, read once (`load`).
static MODELS: OnceLock<Option<Models>> = OnceLock::new();

#[allow(clippy::disallowed_methods)] // The operator's configuration file, not node storage.
fn from_env() -> anyhow::Result<Option<Models>> {
    use std::io::Read as _;
    let Some(path) = crate::env_vars::value(VARIABLE)? else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    if !path.is_absolute() {
        bail!("{VARIABLE} is an absolute path, not {}", path.display());
    }
    let file = std::fs::File::open(&path).map_err(|e| anyhow!("{VARIABLE} {}: {e}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(FILE_MAX_BYTES + 1).read_to_end(&mut bytes).map_err(|e| anyhow!("{VARIABLE} {}: {e}", path.display()))?;
    if bytes.len() as u64 > FILE_MAX_BYTES {
        bail!("{VARIABLE} {} is over {FILE_MAX_BYTES} bytes", path.display());
    }
    Models::parse(&bytes, &path).map(Some)
}

/// Reads and checks the node's AI file, before the node starts: a file
/// that is wrong stops the process, saying why.
pub fn load() -> anyhow::Result<()> {
    if MODELS.get().is_none() {
        let models = from_env()?;
        let _ = MODELS.set(models);
    }
    Ok(())
}

/// `model`'s answer to `inputs`, or why there is none.
pub async fn run(model: &str, inputs: &str) -> Result<Answer, Failure> {
    match MODELS.get_or_init(|| from_env().expect("validated CELLD_AI_FILE")) {
        Some(models) => models.run(model, inputs).await,
        None => Err(failure("unbound", format!("this node has no Workers AI model {model}: name its server in {VARIABLE}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// Valid: the fragment cell's input (audio, task, vad_filter, and a
    /// language and prompt when it has them) is whisper.cpp's form.
    #[test]
    fn the_catalogs_input_is_whisper_cpps_form() {
        let r = whisper_request(&json!({ "audio": b64(b"OggS voice"), "task": "transcribe", "vad_filter": true }).to_string()).unwrap();
        assert_eq!(r.audio, b"OggS voice");
        assert_eq!(r.fields, vec![("language", "auto".into()), ("vad", "true".into()), ("response_format", "verbose_json".into()), ("temperature", "0".into())]);
        assert_eq!(r.language, None, "no language: Whisper detects it");
        let r = whisper_request(&json!({ "audio": b64(b"x"), "language": "de", "initial_prompt": "Paul, fragment", "task": "translate", "beam_size": 5 }).to_string()).unwrap();
        assert!(r.fields.contains(&("language", "de".into())) && r.fields.contains(&("prompt", "Paul, fragment".into())) && r.fields.contains(&("translate", "true".into())) && r.fields.contains(&("beam_size", "5".into())), "{:?}", r.fields);
        assert!(!r.fields.iter().any(|(k, _)| *k == "vad"), "no vad_filter: none");
    }

    /// Invalid: what Whisper's catalog has no input for, or celld does not
    /// translate, is refused by name (400), never let go; and audio that is
    /// none, or past its bound.
    #[test]
    fn an_input_out_of_shape_is_refused() {
        let status = |input: Value| whisper_request(&input.to_string()).err().map(|a| (a.status, a.body["errors"][0]["message"].as_str().unwrap_or("").to_string()));
        assert!(status(json!({})).is_some_and(|(s, m)| s == 400 && m.contains("audio")));
        assert!(status(json!({ "audio": "not base64!" })).is_some_and(|(s, _)| s == 400));
        assert!(status(json!({ "audio": [1, 2, 3] })).is_some_and(|(s, _)| s == 400), "the old Whisper's byte array is not this model's");
        assert!(status(json!({ "audio": "" })).is_some_and(|(s, _)| s == 400));
        assert!(status(json!({ "audio": b64(b"x"), "prefix": "so" })).is_some_and(|(s, m)| s == 400 && m.contains("prefix")));
        assert!(status(json!({ "audio": b64(b"x"), "temperature": 0 })).is_some_and(|(s, m)| s == 400 && m.contains("temperature")));
        assert!(status(json!({ "audio": b64(b"x"), "language": "klingon" })).is_some_and(|(s, _)| s == 400));
        assert!(status(json!({ "audio": b64(b"x"), "task": "summarize" })).is_some_and(|(s, _)| s == 400));
        assert!(status(json!({ "audio": b64(b"x"), "vad_filter": "yes" })).is_some_and(|(s, _)| s == 400));
        assert!(status(json!({ "audio": b64(b"x"), "beam_size": 0 })).is_some_and(|(s, _)| s == 400));
        let big = vec![0u8; AUDIO_MAX_BYTES + 1];
        assert!(status(json!({ "audio": b64(&big) })).is_some_and(|(s, _)| s == 413));
        assert!(whisper_request(&json!({ "audio": b64(&big[..AUDIO_MAX_BYTES]) }).to_string()).is_ok(), "the bound itself is taken");
    }

    /// whisper.cpp's verbose_json is the catalog's answer: its text, words,
    /// segments, a WebVTT, and the language's code and the audio's length;
    /// what it does not measure is left out. One without text or length is
    /// none.
    #[test]
    fn whisper_cpps_answer_is_the_catalogs() {
        let verbose = json!({
            "task": "transcribe", "language": "english", "duration": 3.5, "text": " Hello there.\n General Kenobi.\n",
            "detected_language": "english", "detected_language_probability": 0.97,
            "segments": [
                { "id": 0, "text": " Hello there.", "start": 0.0, "end": 1.2, "tokens": [1], "words": [], "temperature": 0.0, "avg_logprob": -0.2, "no_speech_prob": 0.01 },
                { "id": 1, "text": " General Kenobi.", "start": 1.5, "end": 3.25, "tokens": [2], "temperature": 0.0, "avg_logprob": -0.3, "no_speech_prob": 0.02 }
            ]
        });
        let a = catalog_answer(&verbose, None).unwrap();
        assert_eq!(a["text"], "Hello there. General Kenobi.");
        assert_eq!(a["word_count"], 4);
        assert_eq!(a["transcription_info"], json!({ "language": "en", "language_probability": 0.97, "duration": 3.5 }));
        assert_eq!(a["segments"][1], json!({ "start": 1.5, "end": 3.25, "text": "General Kenobi.", "temperature": 0.0, "avg_logprob": -0.3, "no_speech_prob": 0.02 }));
        assert_eq!(a["vtt"], "WEBVTT\n\n00:00:00.000 --> 00:00:01.200\nHello there.\n\n00:00:01.500 --> 00:00:03.250\nGeneral Kenobi.\n\n");
        assert!(a["transcription_info"].get("duration_after_vad").is_none(), "not measured, not made up");
        let asked = catalog_answer(&verbose, Some("de")).unwrap();
        assert_eq!(asked["transcription_info"], json!({ "language": "de", "duration": 3.5 }), "the language it was told");
        let silent = catalog_answer(&json!({ "text": "", "duration": 2.0, "segments": [] }), None).unwrap();
        assert_eq!((silent["text"].clone(), silent["word_count"].clone()), (json!(""), json!(0)), "silence is no words");
        assert_eq!(catalog_answer(&json!({ "duration": 2.0 }), None), None);
        assert_eq!(catalog_answer(&json!({ "text": "hi" }), None), None);
        assert_eq!(catalog_answer(&json!({ "text": "hi", "duration": -1.0 }), None), None);
    }

    /// The form's boundary is in neither the audio nor a field.
    #[test]
    fn the_forms_boundary_is_its_own() {
        let mut r = whisper_request(&json!({ "audio": b64(b"x") }).to_string()).unwrap();
        let (first, _) = form(&r);
        r.audio = format!("abc{first}def").into_bytes();
        let (boundary, body) = form(&r);
        assert_ne!(boundary, first);
        let text = String::from_utf8_lossy(&body);
        assert!(text.ends_with(&format!("--{boundary}--\r\n")) && text.contains("name=\"file\"; filename=\"audio\""));
    }

    /// The AI file: the one model, its engine and origin; anything else
    /// stops the node, saying what it takes.
    #[test]
    fn the_ai_file_names_whisper_on_whisper_cpp() {
        let f = Path::new("/etc/celld/ai.json");
        let ok = Models::parse(br#"{"models":{"@cf/openai/whisper-large-v3-turbo":{"engine":"whisper.cpp","origin":"http://127.0.0.1:8178","concurrency":2}}}"#, f).unwrap();
        assert_eq!(ok.servers[TRANSCRIBE].origin, "http://127.0.0.1:8178");
        assert_eq!(ok.servers[TRANSCRIBE].slots.available_permits(), 2);
        for bad in [
            r#"[]"#,
            r#"{"models":{}, "extra": 1}"#,
            r#"{"models":{"@cf/meta/llama-3.1-8b-instruct":{"engine":"whisper.cpp","origin":"http://127.0.0.1:8178"}}}"#,
            r#"{"models":{"@cf/openai/whisper-large-v3-turbo":{"engine":"faster-whisper","origin":"http://127.0.0.1:8178"}}}"#,
            r#"{"models":{"@cf/openai/whisper-large-v3-turbo":{"engine":"whisper.cpp","origin":"http://127.0.0.1:8178/inference"}}}"#,
            r#"{"models":{"@cf/openai/whisper-large-v3-turbo":{"engine":"whisper.cpp","origin":"http://127.0.0.1:8178","concurrency":0}}}"#,
            r#"{"models":{"@cf/openai/whisper-large-v3-turbo":{"engine":"whisper.cpp","origin":"http://127.0.0.1:8178","threads":8}}}"#,
        ] {
            assert!(Models::parse(bad.as_bytes(), f).is_err(), "{bad}");
        }
    }

    #[test]
    fn languages_by_code_and_name() {
        assert_eq!(code_of("en"), Some("en"));
        assert_eq!(code_of("english"), Some("en"));
        assert_eq!(code_of("haitian creole"), Some("ht"));
        assert_eq!(code_of("yue"), Some("yue"));
        assert_eq!(code_of("xx"), None);
        assert_eq!(LANGUAGES.split(',').count(), 100);
    }
}
