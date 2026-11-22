// fork: the sealed fleet bucket (docs/services/sealed-bucket.md).
//
// Every object celld keeps is sealed before it leaves the node, so whoever
// holds the bucket (its provider, its project's administrators, anyone with
// its credentials) reads only ciphertext. The store's own index says which
// version of each object is the latest, and its history is a chain whose
// head lives in a counter outside the bucket, behind other credentials: a
// bucket that serves an older object, hides a write or lets a second writer
// win is refused, not believed.
//
// The index is the store's listing: a list or a head is answered from it,
// and the bucket is asked only for bytes. A write is acknowledged once its
// commit has reached the counter; commits group, so concurrent writes share
// one counter write.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::{FutureExt as _, StreamExt as _, TryStreamExt as _};
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, Error, GetOptions, GetRange, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, UpdateVersion,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub mod seal;

use seal::{Head, MasterKey, RecordKey, SALT_BYTES};

const STORE: &str = "sealed bucket";
/// The variable naming the sealed bucket's configuration file.
pub const VARIABLE: &str = "CELLD_BUCKET_SEAL";
/// The configuration file, at most.
const CONFIG_BYTES_MAX: u64 = 16 * 1024;
/// The store's own objects live under this name in the bucket's prefix, and
/// celld never writes one of its own there.
pub const ROOT: &str = "sealed-bucket";
/// Changes one commit carries, at most; more wait for the next.
const BATCH_MAX: usize = 1024;
/// A snapshot of the index every this many commits, so a start replays at
/// most this many journal records.
const SNAPSHOT_EVERY: u64 = 256;
/// Journal records fetched at once at a start.
const JOURNAL_FETCH: usize = 16;
/// Objects the index holds, at most.
pub const ENTRIES_MAX: usize = 4_000_000;
/// A sealed record read whole (a snapshot of the largest index), at most.
const RECORD_BYTES_MAX: usize = 1 << 30;
/// A multipart upload's parts, cut from the sealed stream: above S3's 5 MiB
/// floor.
const PART_BYTES: usize = 8 << 20;
/// A read retried when the index moved under it (a write landed between
/// the read's lookup and its fetch).
const READ_TRIES: usize = 3;

/// Why the store refused: the bucket, or the counter, is not the one this
/// store wrote. Carried as an `object_store::Error::Generic` source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SealError {
    /// The bucket serves bytes this store did not write last under `name`.
    Tampered { name: String, why: String },
    /// The bucket lost what the index holds, or its history does not reach
    /// the counter.
    History(String),
    /// Another writer advanced the counter: this process writes no more.
    Fenced(String),
    /// A name of the store's own, or past a limit.
    Refused(String),
}

impl fmt::Display for SealError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SealError::Tampered { name, why } => write!(f, "the bucket serves a version of {name} this store did not write last ({why})"),
            SealError::History(why) => write!(f, "the sealed bucket's history is not the counter's: {why}"),
            SealError::Fenced(why) => write!(f, "this sealed bucket has another writer, so this process writes no more: {why}"),
            SealError::Refused(why) => f.write_str(why),
        }
    }
}

impl std::error::Error for SealError {}

fn sealed_error(error: SealError) -> Error {
    Error::Generic { store: STORE, source: Box::new(error) }
}

fn generic(message: impl Into<String>) -> Error {
    Error::Generic { store: STORE, source: message.into().into() }
}

fn not_found(name: &str) -> Error {
    Error::NotFound { path: name.to_string(), source: "the object does not exist".into() }
}

/// The sealed bucket's configuration: its key and where its counter is.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigFile {
    /// The bucket's 32-byte key, base64.
    key: String,
    counter: CounterConfig,
}

#[derive(Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct CounterConfig {
    /// `s3://NAME[/PREFIX]`, a bucket other than the fleet's.
    pub bucket: String,
    /// The counter's object in it: one per sealed bucket.
    pub name: String,
    pub region: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Its own credentials, which the fleet bucket's do not reach.
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl fmt::Debug for CounterConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CounterConfig").field("bucket", &self.bucket).field("name", &self.name).finish_non_exhaustive()
    }
}

pub struct Config {
    key: MasterKey,
    pub counter: CounterConfig,
}

impl Config {
    /// The configuration `CELLD_BUCKET_SEAL` names, if it names one: an
    /// owner-only file, checked whole.
    pub fn from_env() -> anyhow::Result<Option<Config>> {
        let Some(path) = std::env::var_os(VARIABLE).filter(|v| !v.is_empty()) else {
            return Ok(None);
        };
        let path = std::path::PathBuf::from(path);
        let bytes = crate::secrets::owner_only(&path, CONFIG_BYTES_MAX, VARIABLE)?;
        Config::parse(&bytes).map(Some)
    }

    pub fn parse(bytes: &[u8]) -> anyhow::Result<Config> {
        let file: ConfigFile = serde_json::from_slice(bytes).map_err(|e| anyhow::anyhow!("{VARIABLE}: {e}"))?;
        let key = seal::unb64::<{ seal::KEY_BYTES }>(file.key.trim())
            .ok_or_else(|| anyhow::anyhow!("{VARIABLE}: `key` is not 32 bytes of base64 (unpadded)"))?;
        anyhow::ensure!(file.counter.bucket.starts_with("s3://"), "{VARIABLE}: the counter's bucket is an s3:// bucket");
        anyhow::ensure!(!file.counter.name.is_empty() && file.counter.name.len() <= 256, "{VARIABLE}: the counter's name is 1 to 256 bytes");
        Ok(Config { key: MasterKey::new(key), counter: file.counter })
    }

    #[cfg(test)]
    pub fn for_test(key: [u8; seal::KEY_BYTES]) -> Config {
        Config {
            key: MasterKey::new(key),
            counter: CounterConfig {
                bucket: "s3://test".into(),
                name: "counter".into(),
                region: "test".into(),
                endpoint: None,
                access_key_id: String::new(),
                secret_access_key: String::new(),
            },
        }
    }
}

/// One sealed store per bucket and prefix in a process: two would be two
/// writers to one counter.
static REGISTRY: Mutex<Option<HashMap<String, SealedStore>>> = Mutex::new(None);

/// The sealed store over `bucket`'s stores, when `CELLD_BUCKET_SEAL` names
/// a configuration; the same one each time this process opens the bucket.
pub(crate) fn wrap(
    bucket: &str,
    prefix: &str,
    store: Arc<dyn ObjectStore>,
    cas_store: Arc<dyn ObjectStore>,
) -> anyhow::Result<Option<SealedStore>> {
    let Some(config) = Config::from_env()? else {
        return Ok(None);
    };
    let id = format!("{bucket}/{prefix}");
    let mut registry = REGISTRY.lock().expect("sealed bucket registry");
    if let Some(sealed) = registry.get_or_insert_with(HashMap::new).get(&id) {
        return Ok(Some(sealed.clone()));
    }
    let counter = crate::bucket::Bucket::open_unsealed(
        &config.counter.bucket,
        config.counter.endpoint.as_deref(),
        &config.counter.region,
        Some(crate::bucket::StaticCredentials {
            access_key_id: config.counter.access_key_id.clone(),
            secret_access_key: config.counter.secret_access_key.clone(),
            session_token: None,
        }),
        Some("sealed-counter"),
    )?;
    let counter_path = counter.path_of(&config.counter.name);
    let sealed = SealedStore::new(store, cas_store, counter.cas_store(), counter_path, root_of(prefix), config.key);
    registry.get_or_insert_with(HashMap::new).insert(id, sealed.clone());
    Ok(Some(sealed))
}

/// The sealed store this process opened for `bucket` (its name) and
/// `prefix`, if it opened one.
pub fn registered(bucket: &str, prefix: &str) -> Option<SealedStore> {
    REGISTRY.lock().expect("sealed bucket registry").as_ref()?.get(&format!("{bucket}/{prefix}")).cloned()
}

pub fn root_of(prefix: &str) -> Path {
    if prefix.is_empty() {
        Path::from(ROOT)
    } else {
        Path::from(prefix).child(ROOT)
    }
}

/// One object's latest version, as the index holds it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    salt: String,
    size: u64,
    /// The bucket's ETag of the sealed object.
    etag: Option<String>,
    version: Option<String>,
    modified_ms: i64,
    attributes: Vec<StoredAttribute>,
}

impl Entry {
    fn salt(&self) -> [u8; SALT_BYTES] {
        seal::unb64::<SALT_BYTES>(&self.salt).expect("an index entry's salt is 32 bytes")
    }

    fn meta(&self, name: &str) -> object_store::Result<ObjectMeta> {
        Ok(ObjectMeta {
            location: Path::parse(name)?,
            last_modified: chrono::DateTime::from_timestamp_millis(self.modified_ms)
                .ok_or_else(|| generic("an index entry's time is out of range"))?,
            size: self.size,
            e_tag: self.etag.clone(),
            version: self.version.clone(),
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StoredAttribute {
    kind: String,
    #[serde(default)]
    name: Option<String>,
    value: String,
}

fn encode_attributes(attributes: &Attributes) -> object_store::Result<Vec<StoredAttribute>> {
    let mut stored = Vec::with_capacity(attributes.len());
    for (attribute, value) in attributes {
        let (kind, name) = match attribute {
            Attribute::ContentDisposition => ("content-disposition", None),
            Attribute::ContentEncoding => ("content-encoding", None),
            Attribute::ContentLanguage => ("content-language", None),
            Attribute::ContentType => ("content-type", None),
            Attribute::CacheControl => ("cache-control", None),
            Attribute::StorageClass => ("storage-class", None),
            Attribute::Metadata(name) => ("metadata", Some(name.as_ref().to_string())),
            _ => return Err(generic("the sealed bucket keeps no such attribute")),
        };
        stored.push(StoredAttribute { kind: kind.into(), name, value: value.as_ref().to_string() });
    }
    Ok(stored)
}

fn decode_attributes(stored: &[StoredAttribute]) -> object_store::Result<Attributes> {
    let mut attributes = Attributes::with_capacity(stored.len());
    for stored in stored {
        let attribute = match stored.kind.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            "metadata" => Attribute::Metadata(stored.name.clone().unwrap_or_default().into()),
            kind => return Err(generic(format!("an index entry holds an unknown attribute {kind:?}"))),
        };
        attributes.insert(attribute, AttributeValue::from(stored.value.clone()));
    }
    Ok(attributes)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
enum Change {
    Put { name: String, entry: Entry },
    Delete { name: String },
}

/// One commit's changes: its bytes are what the chain hashes.
#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct Batch {
    seq: u64,
    at_ms: i64,
    changes: Vec<Change>,
}

/// The counter's value: the latest commit, and its batch, which the bucket's
/// journal may not hold yet.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CounterRecord {
    seq: u64,
    hash: String,
    batch: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    seq: u64,
    hash: String,
    entries: BTreeMap<String, Entry>,
}

struct Pending {
    ticket: u64,
    change: Change,
    /// What the index held before: a snapshot shows the committed state.
    prev: Option<Entry>,
}

/// Where the store's history stands, held by the commit's leader.
struct Chain {
    head: Head,
    /// The counter's ETag, for the next commit's conditional write.
    token: Option<String>,
    /// The last batch, until its journal record is in the bucket.
    journal_due: Option<(u64, Vec<u8>)>,
    since_snapshot: u64,
}

struct Live {
    entries: Mutex<BTreeMap<String, Entry>>,
    pending: Mutex<Vec<Pending>>,
    next_ticket: AtomicU64,
    committed: tokio::sync::watch::Sender<u64>,
    chain: tokio::sync::Mutex<Chain>,
    poisoned: Mutex<Option<SealError>>,
    names: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// The sealed store: a handle, cheap to clone, that listings' streams and
/// uploads hold past the call that made them.
#[derive(Clone)]
pub struct SealedStore(Arc<Core>);

impl std::ops::Deref for SealedStore {
    type Target = Core;

    fn deref(&self) -> &Core {
        &self.0
    }
}

pub struct Core {
    /// The bucket, with retries, and without (for conditional writes).
    inner: Arc<dyn ObjectStore>,
    inner_cas: Arc<dyn ObjectStore>,
    counter: Arc<dyn ObjectStore>,
    counter_path: Path,
    root: Path,
    key: MasterKey,
    records: RecordKey,
    live: tokio::sync::OnceCell<Arc<Live>>,
}

impl fmt::Debug for SealedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealedStore").field("inner", &self.inner.to_string()).field("root", &self.root).finish_non_exhaustive()
    }
}

impl fmt::Display for SealedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SealedStore({})", self.inner)
    }
}

impl SealedStore {
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        inner_cas: Arc<dyn ObjectStore>,
        counter: Arc<dyn ObjectStore>,
        counter_path: Path,
        root: Path,
        key: MasterKey,
    ) -> SealedStore {
        let records = key.records();
        SealedStore(Arc::new(Core { inner, inner_cas, counter, counter_path, root, key, records, live: tokio::sync::OnceCell::new() }))
    }
}

impl Core {
    fn journal_path(&self, seq: u64) -> Path {
        self.root.child("journal").child(format!("{seq:016x}"))
    }

    fn snapshot_path(&self, seq: u64) -> Path {
        self.root.child("snapshot").child(format!("{seq:016x}"))
    }

    fn own(&self, location: &Path) -> bool {
        location.prefix_match(&self.root).is_some()
    }

    async fn live(&self) -> object_store::Result<&Arc<Live>> {
        let live = self.live.get_or_try_init(|| async { self.load().await.map(Arc::new) }).await?;
        if let Some(poison) = live.poisoned.lock().expect("poison").clone() {
            return Err(sealed_error(poison));
        }
        Ok(live)
    }

    /// The counter's record and its ETag, if it has one.
    async fn read_counter(&self) -> object_store::Result<Option<(CounterRecord, Option<String>)>> {
        let result = match self.counter.get(&self.counter_path).await {
            Ok(result) => result,
            Err(Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let etag = result.meta.e_tag.clone();
        let bytes = result.bytes().await?;
        let plain = self.records.open("counter", &bytes).map_err(|e| sealed_error(SealError::History(e.to_string())))?;
        let record: CounterRecord = serde_json::from_slice(&plain).map_err(|e| sealed_error(SealError::History(format!("the counter's record: {e}"))))?;
        Ok(Some((record, etag)))
    }

    async fn read_record(&self, path: &Path, kind: &'static str) -> object_store::Result<Option<Vec<u8>>> {
        let result = match self.inner.get(path).await {
            Ok(result) => result,
            Err(Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        if result.meta.size as usize > RECORD_BYTES_MAX {
            return Err(sealed_error(SealError::History(format!("{path} is {} bytes, past {RECORD_BYTES_MAX}", result.meta.size))));
        }
        let bytes = result.bytes().await?;
        self.records.open(kind, &bytes).map(Some).map_err(|e| sealed_error(SealError::History(format!("{path}: {e}"))))
    }

    /// The index at the counter's head: the newest snapshot that opens, then
    /// every commit after it, each checked against the chain.
    async fn load(&self) -> object_store::Result<Live> {
        let counter = self.read_counter().await?;
        let Some((record, token)) = counter else {
            // A new sealed bucket only when it holds no sealed history.
            if let Some(found) = self.inner.list(Some(&self.root)).next().await {
                let found = found?;
                return Err(sealed_error(SealError::History(format!(
                    "the counter holds nothing, but the bucket holds the sealed history {}: the counter was removed, or this is not its bucket",
                    found.location
                ))));
            }
            return Ok(Live::new(BTreeMap::new(), Chain { head: Head::GENESIS, token: None, journal_due: None, since_snapshot: 0 }));
        };
        let target = Head {
            seq: record.seq,
            hash: seal::unb64::<32>(&record.hash).ok_or_else(|| sealed_error(SealError::History("the counter's hash".into())))?,
        };
        let last_batch = seal::unb64_vec(&record.batch).ok_or_else(|| sealed_error(SealError::History("the counter's batch".into())))?;

        let mut snapshots: Vec<u64> = self
            .inner
            .list(Some(&self.root.child("snapshot")))
            .try_filter_map(|meta| async move { Ok(meta.location.filename().and_then(|n| u64::from_str_radix(n, 16).ok())) })
            .try_collect()
            .await?;
        snapshots.retain(|seq| *seq <= target.seq);
        snapshots.sort_unstable_by(|a, b| b.cmp(a));
        let mut head = Head::GENESIS;
        let mut entries = BTreeMap::new();
        for seq in snapshots {
            let Some(plain) = self.read_record(&self.snapshot_path(seq), "snapshot").await? else { continue };
            let snapshot: Snapshot = serde_json::from_slice(&plain).map_err(|e| sealed_error(SealError::History(format!("snapshot {seq}: {e}"))))?;
            let hash = seal::unb64::<32>(&snapshot.hash).filter(|_| snapshot.seq == seq);
            let Some(hash) = hash else {
                return Err(sealed_error(SealError::History(format!("snapshot {seq} names another place in the chain"))));
            };
            head = Head { seq, hash };
            entries = snapshot.entries;
            break;
        }

        let missing: Vec<u64> = (head.seq + 1..target.seq).collect();
        let fetched: Vec<(u64, Option<Vec<u8>>)> = stream::iter(missing)
            .map(|seq| async move { self.read_record(&self.journal_path(seq), "journal").await.map(|r| (seq, r)) })
            .buffered(JOURNAL_FETCH)
            .try_collect()
            .await?;
        let mut batches: Vec<(u64, Vec<u8>)> = Vec::with_capacity(fetched.len() + 1);
        for (seq, bytes) in fetched {
            let bytes = bytes.ok_or_else(|| sealed_error(SealError::History(format!("the journal's commit {seq} is gone: the bucket lost or hid sealed writes"))))?;
            batches.push((seq, bytes));
        }
        if target.seq > head.seq {
            batches.push((target.seq, last_batch.clone()));
        }
        for (seq, bytes) in batches {
            let batch: Batch = serde_json::from_slice(&bytes).map_err(|e| sealed_error(SealError::History(format!("commit {seq}: {e}"))))?;
            if batch.seq != seq {
                return Err(sealed_error(SealError::History(format!("the journal's record {seq} is commit {}", batch.seq))));
            }
            head = head.next(&bytes);
            for change in batch.changes {
                match change {
                    Change::Put { name, entry } => {
                        entries.insert(name, entry);
                    }
                    Change::Delete { name } => {
                        entries.remove(&name);
                    }
                }
            }
        }
        if head != target {
            return Err(sealed_error(SealError::History(format!(
                "its commits reach {} but the counter is at {}: the bucket served an older or another history",
                head.seq, target.seq
            ))));
        }
        if entries.len() > ENTRIES_MAX {
            return Err(sealed_error(SealError::Refused(format!("the index holds {} objects, past {ENTRIES_MAX}", entries.len()))));
        }
        let journal_due = (target.seq > 0).then_some((target.seq, last_batch));
        Ok(Live::new(entries, Chain { head: target, token, journal_due, since_snapshot: 0 }))
    }

    async fn name_lock(&self, live: &Live, name: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let lock = {
            let mut names = live.names.lock().expect("name locks");
            if names.len() > 4096 {
                names.retain(|_, lock| Arc::strong_count(lock) > 1);
            }
            names.entry(name.to_string()).or_default().clone()
        };
        lock.lock_owned().await
    }

    fn poison(&self, live: &Live, error: SealError) -> Error {
        tracing::error!(event = "sealed_bucket_refused", error = %error, "the sealed bucket refuses every further call");
        *live.poisoned.lock().expect("poison") = Some(error.clone());
        sealed_error(error)
    }

    /// Writes `plain` as `name`'s new version under `mode`, and returns once
    /// the commit that names it has reached the counter.
    async fn write(&self, location: &Path, plain: Bytes, mode: PutMode, attributes: &Attributes) -> object_store::Result<PutResult> {
        if self.own(location) {
            return Err(sealed_error(SealError::Refused(format!("{location} is the sealed bucket's own name"))));
        }
        if plain.len() as u64 > seal::OBJECT_BYTES_MAX {
            return Err(sealed_error(SealError::Refused(format!("an object is at most {} bytes", seal::OBJECT_BYTES_MAX))));
        }
        let live = self.live().await?;
        let name = location.to_string();
        let _lock = self.name_lock(live, &name).await;
        let prev = live.entries.lock().expect("index").get(&name).cloned();
        match (&mode, &prev) {
            (PutMode::Create, Some(_)) => return Err(Error::AlreadyExists { path: name, source: "the object already exists".into() }),
            (PutMode::Update(version), current) if current.as_ref().and_then(|c| c.etag.as_deref()) != version.e_tag.as_deref() || current.is_none() => {
                return Err(Error::Precondition { path: name, source: "the ETag does not match".into() })
            }
            _ => {}
        }
        if prev.is_none() && live.entries.lock().expect("index").len() >= ENTRIES_MAX {
            return Err(sealed_error(SealError::Refused(format!("the sealed bucket holds its most objects ({ENTRIES_MAX})"))));
        }
        let attributes = encode_attributes(attributes)?;
        let mut salt = [0u8; SALT_BYTES];
        getrandom::fill(&mut salt).expect("OS random source unavailable");
        let sealed = seal::seal_object(&self.key.object(&name, &salt), &salt, &plain);
        // Every write is conditional on the version the index holds, so a
        // second writer's object cannot silently replace this one's.
        let bucket_mode = match &prev {
            Some(prev) => PutMode::Update(UpdateVersion { e_tag: prev.etag.clone(), version: prev.version.clone() }),
            None => PutMode::Create,
        };
        let result = match self.inner_cas.put_opts(location, sealed.into(), PutOptions::from(bucket_mode)).await {
            Ok(result) => result,
            Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {
                return Err(self.poison(live, SealError::Tampered { name, why: "the bucket's version is not the index's: another writer, or a rewrite".into() }));
            }
            Err(error) => match self.reconcile(location, &salt).await {
                Some(result) => result,
                None => return Err(error),
            },
        };
        let entry = Entry {
            salt: seal::b64(&salt),
            size: plain.len() as u64,
            etag: result.e_tag.clone(),
            version: result.version.clone(),
            modified_ms: crate::asyncrt::wall_ms(),
            attributes,
        };
        let ticket = self.stage(live, Change::Put { name, entry }, prev);
        self.commit(live, ticket).await?;
        Ok(result)
    }

    /// After a write's ambiguous failure: whether the bucket holds it (its
    /// salt in the header), with its ETag.
    async fn reconcile(&self, location: &Path, salt: &[u8; SALT_BYTES]) -> Option<PutResult> {
        let options = GetOptions { range: Some(GetRange::Bounded(0..seal::HEADER_BYTES)), ..GetOptions::default() };
        let result = self.inner.get_opts(location, options).await.ok()?;
        let (e_tag, version) = (result.meta.e_tag.clone(), result.meta.version.clone());
        let header = result.bytes().await.ok()?;
        (seal::salt_of(&header).ok()? == *salt).then_some(PutResult { e_tag, version })
    }

    /// Applies `change` to the index this process reads, and queues it for
    /// the next commit: its ticket.
    fn stage(&self, live: &Live, change: Change, prev: Option<Entry>) -> u64 {
        let mut pending = live.pending.lock().expect("pending");
        {
            let mut entries = live.entries.lock().expect("index");
            match &change {
                Change::Put { name, entry } => {
                    entries.insert(name.clone(), entry.clone());
                }
                Change::Delete { name } => {
                    entries.remove(name);
                }
            }
        }
        let ticket = live.next_ticket.fetch_add(1, Ordering::SeqCst) + 1;
        pending.push(Pending { ticket, change, prev });
        ticket
    }

    /// Returns once a commit including `ticket` has reached the counter. The
    /// first caller to find no commit running leads one for everything
    /// queued; the others wait for it.
    async fn commit(&self, live: &Live, ticket: u64) -> object_store::Result<()> {
        let mut committed = live.committed.subscribe();
        loop {
            if *committed.borrow_and_update() >= ticket {
                return Ok(());
            }
            if let Some(poison) = live.poisoned.lock().expect("poison").clone() {
                return Err(sealed_error(poison));
            }
            let chain = crate::asyncrt::select! {
                chain = live.chain.lock() => Some(chain),
                changed = committed.changed() => {
                    let _ = changed;
                    None
                }
            };
            if let Some(chain) = chain {
                if *committed.borrow_and_update() >= ticket {
                    return Ok(());
                }
                self.lead(live, chain).await?;
            }
        }
    }

    async fn lead(&self, live: &Live, mut chain: tokio::sync::MutexGuard<'_, Chain>) -> object_store::Result<()> {
        // The last batch's journal record, before the counter forgets it.
        self.persist_journal(&mut chain).await?;
        let drained: Vec<Pending> = {
            let mut pending = live.pending.lock().expect("pending");
            let take = pending.len().min(BATCH_MAX);
            pending.drain(..take).collect()
        };
        let Some(last_ticket) = drained.last().map(|p| p.ticket) else {
            return Ok(());
        };
        let batch = Batch { seq: chain.head.seq + 1, at_ms: crate::asyncrt::wall_ms(), changes: drained.iter().map(|p| p.change.clone()).collect() };
        let bytes = serde_json::to_vec(&batch).expect("a batch serializes");
        let next = chain.head.next(&bytes);
        let record = CounterRecord { seq: next.seq, hash: seal::b64(&next.hash), batch: seal::b64(&bytes) };
        let sealed = self.records.seal("counter", &serde_json::to_vec(&record).expect("a record serializes"));
        let mode = match &chain.token {
            Some(token) => PutMode::Update(UpdateVersion { e_tag: Some(token.clone()), version: None }),
            None => PutMode::Create,
        };
        let token = match self.counter.put_opts(&self.counter_path, sealed.into(), PutOptions::from(mode)).await {
            Ok(result) => result.e_tag,
            Err(Error::AlreadyExists { .. } | Error::Precondition { .. }) => {
                return Err(self.poison(live, SealError::Fenced("the counter moved past this process's last commit".into())));
            }
            Err(error) => match self.read_counter().await {
                Ok(Some((now, etag))) if now.seq == next.seq && now.hash == record.hash => etag,
                Ok(Some((now, _))) if now.seq == chain.head.seq && now.hash == seal::b64(&chain.head.hash) => {
                    // Not applied: the batch goes back, first, for the next leader.
                    live.pending.lock().expect("pending").splice(0..0, drained);
                    return Err(error);
                }
                _ => return Err(self.poison(live, SealError::Fenced(format!("the counter's write was ambiguous ({error}) and the counter is elsewhere now")))),
            },
        };
        chain.head = next;
        chain.token = token;
        chain.journal_due = Some((next.seq, bytes));
        chain.since_snapshot += 1;
        live.committed.send_modify(|watermark| *watermark = (*watermark).max(last_ticket));
        // Off the waiters' path now: they are answered.
        if let Err(error) = self.persist_journal(&mut chain).await {
            tracing::warn!(event = "sealed_bucket_journal_deferred", error = %error, "the next commit writes it");
        }
        if chain.since_snapshot >= SNAPSHOT_EVERY {
            if let Err(error) = self.snapshot(live, &mut chain).await {
                tracing::warn!(event = "sealed_bucket_snapshot_deferred", error = %error, "the next commit tries again");
            }
        }
        Ok(())
    }

    async fn persist_journal(&self, chain: &mut Chain) -> object_store::Result<()> {
        if let Some((seq, bytes)) = chain.journal_due.take() {
            let sealed = self.records.seal("journal", &bytes);
            if let Err(error) = self.inner.put(&self.journal_path(seq), sealed.into()).await {
                chain.journal_due = Some((seq, bytes));
                return Err(error);
            }
        }
        Ok(())
    }

    /// The committed index (the live one, minus what is still queued) at the
    /// chain's head, then the journal before the previous snapshot dropped.
    async fn snapshot(&self, live: &Live, chain: &mut Chain) -> object_store::Result<()> {
        let mut entries = live.entries.lock().expect("index").clone();
        for pending in live.pending.lock().expect("pending").iter().rev() {
            let name = match &pending.change {
                Change::Put { name, .. } | Change::Delete { name } => name.clone(),
            };
            match &pending.prev {
                Some(prev) => entries.insert(name, prev.clone()),
                None => entries.remove(&name),
            };
        }
        let snapshot = Snapshot { seq: chain.head.seq, hash: seal::b64(&chain.head.hash), entries };
        let sealed = self.records.seal("snapshot", &serde_json::to_vec(&snapshot).expect("a snapshot serializes"));
        self.inner.put(&self.snapshot_path(chain.head.seq), sealed.into()).await?;
        chain.since_snapshot = 0;
        // Two snapshots stay; the journal before the older one goes.
        let mut kept: Vec<u64> = self
            .inner
            .list(Some(&self.root.child("snapshot")))
            .try_filter_map(|meta| async move { Ok(meta.location.filename().and_then(|n| u64::from_str_radix(n, 16).ok())) })
            .try_collect()
            .await?;
        kept.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&floor) = kept.get(1) else { return Ok(()) };
        let mut old: Vec<Path> = kept.iter().skip(2).map(|seq| self.snapshot_path(*seq)).collect();
        let journal: Vec<ObjectMeta> = self.inner.list(Some(&self.root.child("journal"))).try_collect().await?;
        old.extend(journal.into_iter().filter(|m| m.location.filename().and_then(|n| u64::from_str_radix(n, 16).ok()).is_some_and(|seq| seq <= floor)).map(|m| m.location));
        for path in old {
            self.inner.delete(&path).await?;
        }
        Ok(())
    }

    /// Reads `name`'s latest version: its bytes in `options.range`, checked
    /// chunk by chunk against the version the index names.
    async fn read(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if self.own(location) {
            return Err(not_found(location.as_ref()));
        }
        let live = self.live().await?;
        let name = location.to_string();
        for _ in 0..READ_TRIES {
            let Some(entry) = live.entries.lock().expect("index").get(&name).cloned() else {
                return Err(not_found(&name));
            };
            let meta = entry.meta(&name)?;
            options.check_preconditions(&meta)?;
            let attributes = decode_attributes(&entry.attributes)?;
            let range = match &options.range {
                Some(range) => range.as_range(entry.size).map_err(|e| generic(e.to_string()))?,
                None => 0..entry.size,
            };
            if options.head {
                return Ok(GetResult { payload: GetResultPayload::Stream(stream::empty().boxed()), meta, range, attributes });
            }
            let (_, bytes) = seal::span(entry.size, &range);
            let fetch = GetOptions {
                range: Some(GetRange::Bounded(bytes)),
                if_match: entry.etag.clone(),
                version: entry.version.clone(),
                ..GetOptions::default()
            };
            let result = match self.inner.get_opts(location, fetch).await {
                Ok(result) => result,
                Err(error @ (Error::Precondition { .. } | Error::NotFound { .. })) => {
                    let moved = live.entries.lock().expect("index").get(&name) != Some(&entry);
                    if moved {
                        continue;
                    }
                    let why = match error {
                        Error::NotFound { .. } => "the bucket lost it",
                        _ => "its ETag is not the index's",
                    };
                    return Err(sealed_error(SealError::Tampered { name, why: why.into() }));
                }
                Err(error) => return Err(error),
            };
            let opener = seal::Opener::new(self.key.object(&name, &entry.salt()), entry.size, range.clone());
            let opened = opened_stream(result.into_stream(), opener, name.clone());
            return Ok(GetResult { payload: GetResultPayload::Stream(opened), meta, range, attributes });
        }
        Err(sealed_error(SealError::Refused(format!("{name} kept changing under {READ_TRIES} reads"))))
    }

    async fn remove(&self, location: &Path) -> object_store::Result<()> {
        if self.own(location) {
            return Err(sealed_error(SealError::Refused(format!("{location} is the sealed bucket's own name"))));
        }
        let live = self.live().await?;
        let name = location.to_string();
        let _lock = self.name_lock(live, &name).await;
        let prev = live.entries.lock().expect("index").get(&name).cloned();
        self.inner.delete(location).await?;
        if prev.is_some() {
            let ticket = self.stage(live, Change::Delete { name }, prev);
            self.commit(live, ticket).await?;
        }
        Ok(())
    }

    /// The index's objects under `prefix` (a key prefix, not a path's).
    async fn listed(&self, prefix: &str) -> object_store::Result<Vec<ObjectMeta>> {
        let live = self.live().await?;
        let entries = live.entries.lock().expect("index");
        entries.range(prefix.to_string()..).take_while(|(name, _)| name.starts_with(prefix)).map(|(name, entry)| entry.meta(name)).collect()
    }

    async fn whole(&self, location: &Path) -> object_store::Result<(Bytes, Attributes)> {
        let result = self.read(location, GetOptions::default()).await?;
        let attributes = result.attributes.clone();
        Ok((result.bytes().await?, attributes))
    }

    /// The commit the store stands at.
    pub async fn commit_head(&self) -> object_store::Result<Head> {
        Ok(self.live().await?.chain.lock().await.head)
    }
}

impl Live {
    fn new(entries: BTreeMap<String, Entry>, chain: Chain) -> Live {
        Live {
            entries: Mutex::new(entries),
            pending: Mutex::new(Vec::new()),
            next_ticket: AtomicU64::new(0),
            committed: tokio::sync::watch::channel(0).0,
            chain: tokio::sync::Mutex::new(chain),
            poisoned: Mutex::new(None),
            names: Mutex::new(HashMap::new()),
        }
    }
}

/// The plaintext of a fetched span, opened as its bytes arrive.
fn opened_stream(
    sealed: BoxStream<'static, object_store::Result<Bytes>>,
    opener: seal::Opener,
    name: String,
) -> BoxStream<'static, object_store::Result<Bytes>> {
    stream::unfold((sealed, Some(opener), name), |(mut sealed, opener, name)| async move {
        let mut opener = opener?;
        loop {
            match sealed.next().await {
                Some(Ok(bytes)) => match opener.push(&bytes) {
                    Ok(plain) if plain.is_empty() => continue,
                    Ok(plain) => return Some((Ok(Bytes::from(plain)), (sealed, Some(opener), name))),
                    Err(why) => {
                        let error = sealed_error(SealError::Tampered { name: name.clone(), why: why.to_string() });
                        return Some((Err(error), (sealed, None, name)));
                    }
                },
                Some(Err(error)) => return Some((Err(error), (sealed, None, name))),
                None => {
                    return match opener.finish() {
                        Ok(()) => None,
                        Err(why) => {
                            let error = sealed_error(SealError::Tampered { name: name.clone(), why: why.to_string() });
                            Some((Err(error), (sealed, None, name)))
                        }
                    }
                }
            }
        }
    })
    // A reader may ask once more after the end (`collect_bytes` does, for an
    // empty object): fused, the stream answers it with its end again.
    .fuse()
    .boxed()
}

#[async_trait]
impl ObjectStore for SealedStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, options: PutOptions) -> object_store::Result<PutResult> {
        self.write(location, payload.into(), options.mode, &options.attributes).await
    }

    async fn put_multipart_opts(&self, location: &Path, options: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        if self.own(location) {
            return Err(sealed_error(SealError::Refused(format!("{location} is the sealed bucket's own name"))));
        }
        let live = self.live().await?.clone();
        let name = location.to_string();
        let lock = self.name_lock(&live, &name).await;
        let attributes = encode_attributes(&options.attributes)?;
        let mut salt = [0u8; SALT_BYTES];
        getrandom::fill(&mut salt).expect("OS random source unavailable");
        let inner = self.inner.put_multipart_opts(location, PutMultipartOptions::default()).await?;
        Ok(Box::new(SealedUpload {
            store: self.clone(),
            live,
            name: name.clone(),
            salt,
            key: Arc::new(self.key.object(&name, &salt)),
            sealer_state: SealerState::new(),
            out: Vec::new(),
            size: 0,
            attributes,
            inner,
            _lock: lock,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.read(location, options).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.remove(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let this = self.clone();
        stream::once(async move {
            let scan = prefix.as_ref().map_or(String::new(), |p| p.to_string());
            let listed = this.listed(&scan).await?;
            Ok::<_, Error>(stream::iter(
                listed
                    .into_iter()
                    .filter(move |meta| {
                        prefix.as_ref().is_none_or(|prefix| meta.location.prefix_match(prefix).is_some_and(|mut rest| rest.next().is_some()))
                    })
                    .map(Ok),
            ))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for meta in self.listed(prefix.as_ref()).await? {
            let location = meta.location.clone();
            let Some(mut rest) = location.prefix_match(&prefix) else { continue };
            let Some(child) = rest.next() else { continue };
            if rest.next().is_some() {
                common_prefixes.insert(prefix.child(child));
            } else {
                objects.push(meta);
            }
        }
        Ok(ListResult { common_prefixes: common_prefixes.into_iter().collect(), objects })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let (body, attributes) = self.whole(from).await?;
        self.write(to, body, PutMode::Overwrite, &attributes).await.map(|_| ())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let (body, attributes) = self.whole(from).await?;
        self.write(to, body, PutMode::Create, &attributes).await.map(|_| ())
    }
}

#[async_trait]
impl PaginatedListStore for SealedStore {
    async fn list_paginated(&self, prefix: Option<&str>, options: PaginatedListOptions) -> object_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let delimiter = options.delimiter.as_deref();
        let limit = options.max_keys.unwrap_or(usize::MAX);
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for meta in self.listed(prefix).await? {
            let key = meta.location.to_string();
            let Some(remainder) = key.strip_prefix(prefix) else { continue };
            if after.is_some_and(|after| key.as_str() <= after) {
                continue;
            }
            let common = delimiter.and_then(|d| remainder.find(d).map(|i| format!("{prefix}{}{d}", &remainder[..i])));
            if let Some(common) = common {
                let common = Path::parse(&common)?;
                if let Some((last, Some(previous), _)) = entries.last_mut() {
                    if *previous == common {
                        *last = key;
                        continue;
                    }
                }
                entries.push((key, Some(common), None));
            } else {
                entries.push((key, None, Some(meta)));
            }
        }
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let page_token = truncated.then(|| entries.last().map(|(last, _, _)| last.clone())).flatten();
        let mut result = ListResult { common_prefixes: Vec::new(), objects: Vec::new() };
        for (_, common, object) in entries {
            result.common_prefixes.extend(common);
            result.objects.extend(object);
        }
        Ok(PaginatedListResult { result, page_token })
    }
}

/// A sealed object written as its parts arrive.
struct SealedUpload {
    store: SealedStore,
    live: Arc<Live>,
    name: String,
    salt: [u8; SALT_BYTES],
    key: Arc<seal::ObjectKey>,
    sealer_state: SealerState,
    out: Vec<u8>,
    size: u64,
    attributes: Vec<StoredAttribute>,
    inner: Box<dyn MultipartUpload>,
    _lock: tokio::sync::OwnedMutexGuard<()>,
}

impl fmt::Debug for SealedUpload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SealedUpload").field("name", &self.name).field("size", &self.size).finish_non_exhaustive()
    }
}

/// A `Sealer`'s state apart from its key, so an upload can own both.
struct SealerState {
    header_sent: bool,
    next: u64,
    pending: Vec<u8>,
}

impl SealerState {
    fn new() -> Self {
        Self { header_sent: false, next: 0, pending: Vec::new() }
    }

    fn push(&mut self, key: &seal::ObjectKey, salt: &[u8; SALT_BYTES], plain: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        if !self.header_sent {
            out.extend_from_slice(&seal::header(salt));
            self.header_sent = true;
        }
        self.pending.extend_from_slice(plain);
        let chunk = seal::CHUNK_BYTES as usize;
        let mut taken = 0;
        while self.pending.len() - taken > chunk {
            out.extend(key.seal_chunk(self.next, false, &self.pending[taken..taken + chunk]));
            self.next += 1;
            taken += chunk;
        }
        self.pending.drain(..taken);
        out
    }

    fn finish(&mut self, key: &seal::ObjectKey, salt: &[u8; SALT_BYTES]) -> Vec<u8> {
        let mut out = self.push(key, salt, &[]);
        out.extend(key.seal_chunk(self.next, true, &self.pending));
        self.pending.clear();
        out
    }
}

#[async_trait]
impl MultipartUpload for SealedUpload {
    fn put_part(&mut self, data: PutPayload) -> BoxFuture<'static, object_store::Result<()>> {
        let plain: Bytes = data.into();
        self.size += plain.len() as u64;
        let sealed = self.sealer_state.push(&self.key, &self.salt, &plain);
        self.out.extend(sealed);
        let mut parts = Vec::new();
        while self.out.len() >= PART_BYTES {
            let rest = self.out.split_off(PART_BYTES);
            let part = std::mem::replace(&mut self.out, rest);
            parts.push(self.inner.put_part(part.into()));
        }
        async move {
            for part in parts {
                part.await?;
            }
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.size > seal::OBJECT_BYTES_MAX {
            return Err(sealed_error(SealError::Refused(format!("an object is at most {} bytes", seal::OBJECT_BYTES_MAX))));
        }
        let last = self.sealer_state.finish(&self.key, &self.salt);
        self.out.extend(last);
        let part = std::mem::take(&mut self.out);
        self.inner.put_part(part.into()).await?;
        let result = self.inner.complete().await?;
        let prev = self.live.entries.lock().expect("index").get(&self.name).cloned();
        let entry = Entry {
            salt: seal::b64(&self.salt),
            size: self.size,
            etag: result.e_tag.clone(),
            version: result.version.clone(),
            modified_ms: crate::asyncrt::wall_ms(),
            attributes: self.attributes.clone(),
        };
        let ticket = self.store.stage(&self.live, Change::Put { name: self.name.clone(), entry }, prev);
        self.store.commit(&self.live, ticket).await?;
        Ok(result)
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests;
