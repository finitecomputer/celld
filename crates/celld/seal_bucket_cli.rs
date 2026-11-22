// fork: the sealed fleet bucket's command (docs/services/sealed-bucket.md).
// It reads operator input and object bodies outside node storage.
#![allow(clippy::disallowed_methods)]

//! `celld seal-bucket`: move a fleet bucket into its sealed form, and check
//! a sealed one (what they say goes to stderr, as every celld command's
//! words do). Both take the sealed bucket's key and counter from
//! `CELLD_BUCKET_SEAL`, as a node does, and need no node: run them with the
//! node stopped.
//!
//! `migrate` copies every object of the plain bucket into the sealed one
//! (a new prefix, or another bucket), with its attributes, then reads both
//! back and compares their SHA-256 before it writes its mark. It never
//! deletes the plain objects. Run again, it skips what it has copied, and
//! once marked it does nothing.

use crate::bucket::{split_spec, Bucket};
use crate::note;
use anyhow::{bail, Context as _};
use bytes::Bytes;
use futures_util::{StreamExt as _, TryStreamExt as _};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

const HELP: &str = "celld seal-bucket: a fleet bucket's sealed form

USAGE:
  celld seal-bucket migrate --from s3://NAME[/PREFIX] --to s3://NAME/PREFIX [--region R] [--endpoint URL]
  celld seal-bucket check --bucket s3://NAME/PREFIX [--region R] [--endpoint URL]

CELLD_BUCKET_SEAL names the sealed bucket's configuration (its key and its
counter). Credentials for the buckets come from the environment, as for a node.";

/// The name `migrate` marks a finished migration with, at the sealed
/// bucket's prefix. celld reads no such object.
pub const MARK: &str = "seal-migrated.json";
/// Objects copied at once.
const COPIES: usize = 8;
/// An object read whole to copy it, at most; a larger one streams.
const WHOLE_BYTES_MAX: u64 = 64 << 20;
/// A streamed copy's parts.
const PART_BYTES: usize = 8 << 20;

struct Flags {
    from: Option<String>,
    to: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
}

fn parse(arguments: Vec<String>) -> anyhow::Result<(String, Flags)> {
    let mut arguments = arguments.into_iter();
    let action = arguments.next().unwrap_or_default();
    let mut flags = Flags { from: None, to: None, bucket: None, region: None, endpoint: None };
    while let Some(flag) = arguments.next() {
        let mut value = || arguments.next().with_context(|| format!("{flag} takes a value"));
        match flag.as_str() {
            "--from" => flags.from = Some(value()?),
            "--to" => flags.to = Some(value()?),
            "--bucket" => flags.bucket = Some(value()?),
            "--region" => flags.region = Some(value()?),
            "--endpoint" => flags.endpoint = Some(value()?),
            other => bail!("celld seal-bucket: unknown flag {other}\n\n{HELP}"),
        }
    }
    flags.region = flags.region.or_else(|| std::env::var("AWS_REGION").ok());
    Ok((action, flags))
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let (action, flags) = parse(arguments)?;
    if crate::sealed_bucket::Config::from_env()?.is_none() {
        bail!("celld seal-bucket needs {} (the sealed bucket's key and counter)", crate::sealed_bucket::VARIABLE);
    }
    let region = flags.region.clone().context("celld seal-bucket needs --region (or AWS_REGION)")?;
    match action.as_str() {
        "migrate" => {
            let from = flags.from.context("migrate needs --from")?;
            let to = flags.to.context("migrate needs --to")?;
            migrate(&from, &to, &region, flags.endpoint.as_deref()).await
        }
        "check" => check(&flags.bucket.context("check needs --bucket")?, &region, flags.endpoint.as_deref()).await,
        "" | "help" | "--help" | "-h" => {
            note!("{HELP}");
            Ok(())
        }
        other => bail!("celld seal-bucket: unknown action {other}\n\n{HELP}"),
    }
}

/// `location`'s place under `to` when it is under `from`: its encoded key
/// moved, never encoded again.
fn moved(location: &Path, from: &Path, to: &Path) -> Option<Path> {
    location.prefix_match(from)?.next()?;
    let rest = location.as_ref().strip_prefix(from.as_ref())?.trim_start_matches('/');
    let moved = if to.as_ref().is_empty() { rest.to_string() } else { format!("{to}/{rest}") };
    Path::parse(moved).ok()
}

async fn listing(store: &Arc<dyn ObjectStore>, root: &Path) -> anyhow::Result<Vec<ObjectMeta>> {
    let prefix = (!root.as_ref().is_empty()).then_some(root);
    Ok(store.list(prefix).try_collect().await?)
}

async fn digest(store: &Arc<dyn ObjectStore>, location: &Path) -> anyhow::Result<([u8; 32], u64)> {
    let mut stream = store.get(location).await.with_context(|| format!("read {location}"))?.into_stream();
    let (mut hash, mut bytes) = (Sha256::new(), 0u64);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.with_context(|| format!("read {location}"))?;
        bytes += chunk.len() as u64;
        hash.update(&chunk);
    }
    Ok((hash.finalize().into(), bytes))
}

async fn copy(from: &Arc<dyn ObjectStore>, to: &Arc<dyn ObjectStore>, source: &ObjectMeta, target: &Path) -> anyhow::Result<()> {
    let got = from.get(&source.location).await.with_context(|| format!("read {}", source.location))?;
    let attributes = got.attributes.clone();
    if source.size <= WHOLE_BYTES_MAX {
        let bytes = got.bytes().await.with_context(|| format!("read {}", source.location))?;
        to.put_opts(target, bytes.into(), PutOptions { attributes, ..PutOptions::default() }).await.with_context(|| format!("write {target}"))?;
        return Ok(());
    }
    let mut upload = to.put_multipart_opts(target, PutMultipartOptions { attributes, ..PutMultipartOptions::default() }).await?;
    let mut stream = got.into_stream();
    let mut part: Vec<u8> = Vec::with_capacity(PART_BYTES);
    while let Some(chunk) = stream.next().await {
        part.extend_from_slice(&chunk?);
        if part.len() >= PART_BYTES {
            upload.put_part(Bytes::from(std::mem::take(&mut part)).into()).await?;
        }
    }
    upload.put_part(Bytes::from(part).into()).await?;
    upload.complete().await.with_context(|| format!("write {target}"))?;
    Ok(())
}

async fn migrate(from: &str, to: &str, region: &str, endpoint: Option<&str>) -> anyhow::Result<()> {
    let (_, from_name, _) = split_spec(from);
    let (_, to_name, to_prefix) = split_spec(to);
    if to_prefix.is_empty() && from_name == to_name {
        bail!("--to names the plain bucket itself: give the sealed form a prefix of its own (s3://{to_name}/sealed)");
    }
    let plain = Bucket::open_unsealed(from, endpoint, region, None, Some("seal-migrate"))?;
    let sealed = crate::fleet::bucket_client(to, endpoint, region)?;
    let (plain_root, sealed_root) = (plain.path_of(""), sealed.path_of(""));
    let mark = sealed.path_of(MARK);
    if sealed.store.head(&mark).await.is_ok() {
        note!("{to} is sealed already ({MARK} is there): nothing to do");
        return Ok(());
    }
    // The sealed form may sit inside the plain bucket: it is not copied.
    let nested = from_name == to_name;
    let sources: Vec<(ObjectMeta, Path)> = listing(&plain.store, &plain_root)
        .await?
        .into_iter()
        .filter(|meta| !(nested && meta.location.prefix_match(&sealed_root).is_some()))
        .filter_map(|meta| {
            let target = moved(&meta.location, &plain_root, &sealed_root)?;
            Some((meta, target))
        })
        .collect();
    let have: BTreeMap<Path, u64> = listing(&sealed.store, &sealed_root).await?.into_iter().map(|m| (m.location, m.size)).collect();
    let todo: Vec<&(ObjectMeta, Path)> = sources.iter().filter(|(meta, target)| have.get(target) != Some(&meta.size)).collect();
    note!("{} objects in {from}; {} to copy into {to}", sources.len(), todo.len());
    let copied: Vec<()> = futures_util::stream::iter(todo)
        .map(|(meta, target)| copy(&plain.store, &sealed.store, meta, target))
        .buffer_unordered(COPIES)
        .try_collect()
        .await?;
    note!("copied {}", copied.len());

    // Every object, read back from both and compared.
    let mut bytes = 0u64;
    for (meta, target) in &sources {
        let (a, n) = digest(&plain.store, &meta.location).await?;
        let (b, m) = digest(&sealed.store, target).await?;
        if a != b || n != m {
            bail!("{target} is not {}: {m} bytes against {n} (the migration stops; nothing is deleted)", meta.location);
        }
        bytes += n;
    }
    let listed = listing(&sealed.store, &sealed_root).await?.len();
    if listed != sources.len() {
        bail!("the sealed form lists {listed} objects, the plain {}: the migration stops", sources.len());
    }
    let record = serde_json::json!({ "objects": sources.len(), "bytes": bytes, "from": from_name, "at_ms": crate::asyncrt::wall_ms() });
    sealed.store.put(&mark, Bytes::from(record.to_string()).into()).await?;
    note!("sealed: {} objects, {bytes} bytes, each read back and equal to its plain copy; the plain objects are untouched", sources.len());
    Ok(())
}

async fn check(spec: &str, region: &str, endpoint: Option<&str>) -> anyhow::Result<()> {
    let bucket = crate::fleet::bucket_client(spec, endpoint, region)?;
    let root = bucket.path_of("");
    let (mut objects, mut bytes, mut adopted) = (0u64, 0u64, 0u64);
    for meta in listing(&bucket.store, &root).await? {
        let (_, n) = digest(&bucket.store, &meta.location).await?;
        // A read adopts the home's own version cut short (sealed_bucket.rs,
        // `adopt`): the index's entry may be another after it.
        let now = match bucket.store.head(&meta.location).await {
            Ok(now) => Some(now),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(error) => return Err(error).with_context(|| format!("head {}", meta.location)),
        };
        if now.as_ref().map(|m| (m.size, &m.e_tag)) != Some((meta.size, &meta.e_tag)) {
            adopted += 1;
        }
        if let Some(now) = &now {
            if n != now.size {
                bail!("{} reads {n} bytes, its index says {}", meta.location, now.size);
            }
        }
        objects += 1;
        bytes += n;
    }
    let (_, name, prefix) = split_spec(spec);
    let head = crate::sealed_bucket::registered(name, &prefix).context("the bucket opened unsealed")?.commit_head().await?;
    note!("{spec}: {objects} objects, {bytes} bytes, every one opened under the index's version ({adopted} adopted: the home's own writes cut short); the counter is at commit {}", head.seq);
    Ok(())
}
