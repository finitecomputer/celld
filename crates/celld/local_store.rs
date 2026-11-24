// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The persistent object store for `celld dev`.
//!
//! The development supervisor and its node are separate processes, and both
//! write the fleet store during a reload. SQLite supplies the cross-process
//! transaction that a directory of files cannot: a conditional update checks
//! its ETag and installs the new object in one commit. This backend is local to
//! one development machine. It is not a shared-filesystem production mode.
//!
//! fork: the node leases (`nodes/`) live in a database of their own beside the
//! objects. SQLite admits one writer per database, and every write here is a
//! commit that waits on the disk, so a renewal in the shared file queued
//! behind every LTX, R2 and deployment write the node made. On a busy disk
//! that wait outran the renewal's bound, and the node fenced itself
//! (docs/upstream-issues/dev-self-fence.md). A fleet keeps the same lane apart
//! with a client of its own (`fleet::lease_bucket_client_with_credentials`),
//! and its object store never makes one key's write wait for another's.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, Error, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use rusqlite::{params, Connection, OptionalExtension as _, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const STORE: &str = "celld development store";

/// The key space of the node leases, `nodes/{node}.json`.
const LEASES: &str = "nodes/";

/// The first key past [`LEASES`] in byte order: `0` follows `/`.
const LEASES_END: &str = "nodes0";

/// The leases' database, beside the objects' one.
const LEASE_DATABASE: &str = "leases.sqlite3";

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    /// Every object outside [`LEASES`].
    database: PathBuf,
    /// The objects in [`LEASES`].
    leases: PathBuf,
}

#[derive(Debug)]
struct StoredObject {
    key: String,
    body: Vec<u8>,
    size: u64,
    etag: i64,
    modified_ms: i64,
    attributes: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredAttribute {
    kind: String,
    name: Option<String>,
    value: String,
}

impl LocalStore {
    /// The store whose objects live in `database`, and whose leases live in
    /// [`LEASE_DATABASE`] beside it.
    pub(crate) fn open(database: impl AsRef<FsPath>) -> object_store::Result<Self> {
        let database = database.as_ref().to_path_buf();
        let leases = database.with_file_name(LEASE_DATABASE);
        let store = Self { database, leases };
        for database in [&store.database, &store.leases] {
            create_schema(&connect(database)?)?;
        }
        store.move_leases_from_objects()?;
        Ok(store)
    }

    /// The database that holds `key`.
    fn database_of(&self, key: &str) -> &FsPath {
        if key.starts_with(LEASES) {
            &self.leases
        } else {
            &self.database
        }
    }

    /// Moves the leases a celld before the split kept among the objects into
    /// their own database. A move cut short finishes at the next open: a
    /// lease already in its own database wins, and a copy among the objects
    /// is deleted only after the leases' commit, and only as it was copied.
    fn move_leases_from_objects(&self) -> object_store::Result<()> {
        let objects = connect(&self.database)?;
        let mut statement = objects
            .prepare(
                "SELECT key, body, etag, modified_ms, attributes FROM objects
                 WHERE key >= ?1 AND key < ?2",
            )
            .map_err(db_error)?;
        let moved = statement
            .query_map([LEASES, LEASES_END], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(db_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db_error)?;
        drop(statement);
        if moved.is_empty() {
            return Ok(());
        }
        let next_etag: i64 = objects
            .query_row(
                "SELECT next_etag FROM store_sequence WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        let mut leases = connect(&self.leases)?;
        let transaction = leases
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        for (key, body, etag, modified_ms, attributes) in &moved {
            transaction
                .execute(
                    "INSERT OR IGNORE INTO objects(key, body, etag, modified_ms, attributes)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![key, body, etag, modified_ms, attributes],
                )
                .map_err(db_error)?;
        }
        // A holder of an ETag the objects' sequence issued must never match a
        // later version, so the leases' sequence goes on above it.
        transaction
            .execute(
                "UPDATE store_sequence SET next_etag = max(next_etag, ?1) WHERE singleton = 1",
                [next_etag],
            )
            .map_err(db_error)?;
        transaction.commit().map_err(db_error)?;
        for (key, _, etag, _, _) in &moved {
            objects
                .execute(
                    "DELETE FROM objects WHERE key = ?1 AND etag = ?2",
                    params![key, etag],
                )
                .map_err(db_error)?;
        }
        Ok(())
    }

    fn read(&self, key: &str) -> object_store::Result<StoredObject> {
        connect(self.database_of(key))?
            .query_row(
                "SELECT key, body, etag, modified_ms, attributes, length(body)
                 FROM objects WHERE key = ?1",
                [key],
                |row| {
                    Ok(StoredObject {
                        key: row.get(0)?,
                        body: row.get(1)?,
                        size: row.get(5)?,
                        etag: row.get(2)?,
                        modified_ms: row.get(3)?,
                        attributes: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))
    }

    /// Every object whose key starts with `prefix`, in key order, without its
    /// body.
    ///
    /// Stopgap, on the fork only until upstream lists by prefix: this read
    /// every row and left the prefix to its callers, and reading a row's
    /// `attributes`, stored after its body, walks the body's overflow pages.
    /// Each listing read the whole bucket, LTX included. The keys that share a
    /// prefix are one run of the primary key's index (BINARY order is byte
    /// order), so the scan starts at `prefix` and stops at the first key past
    /// it. The callers still filter as before.
    ///
    /// Each database is read only when its key space can hold `prefix`: a
    /// prefix inside [`LEASES`] names no other object, and one that does not
    /// lead into it names no lease.
    fn metadata(&self, prefix: &str) -> object_store::Result<Vec<StoredObject>> {
        let in_leases = prefix.starts_with(LEASES);
        let mut objects = if in_leases {
            Vec::new()
        } else {
            metadata_in(&self.database, prefix)?
        };
        if in_leases || LEASES.starts_with(prefix) {
            // A lease that a celld before the split wrote among the objects
            // after the move is not one this store reads, so no listing shows
            // it either.
            objects.retain(|object| !object.key.starts_with(LEASES));
            objects.extend(metadata_in(&self.leases, prefix)?);
            objects.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        }
        Ok(objects)
    }

    fn put_sync(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let attributes = encode_attributes(&options.attributes)?;
        let modified_ms = crate::asyncrt::wall_ms();
        let mut connection = connect(self.database_of(&key))?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let current = transaction
            .query_row("SELECT etag FROM objects WHERE key = ?1", [&key], |row| {
                row.get::<_, i64>(0)
            })
            .optional()
            .map_err(db_error)?;
        match options.mode {
            PutMode::Overwrite => {}
            PutMode::Create if current.is_some() => return Err(already_exists(&key)),
            PutMode::Create => {}
            PutMode::Update(version) => {
                let current = current.map(|etag| etag.to_string());
                if current.as_deref().is_none() || version.e_tag.as_deref() != current.as_deref() {
                    return Err(precondition(&key));
                }
            }
        }
        let etag = transaction
            .query_row(
                "UPDATE store_sequence SET next_etag = next_etag + 1
                 WHERE singleton = 1 RETURNING next_etag - 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(db_error)?;
        transaction
            .execute(
                "INSERT INTO objects(key, body, etag, modified_ms, attributes)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(key) DO UPDATE SET
                   body = excluded.body,
                   etag = excluded.etag,
                   modified_ms = excluded.modified_ms,
                   attributes = excluded.attributes",
                params![key, body.as_ref(), etag, modified_ms, attributes],
            )
            .map_err(db_error)?;
        transaction.commit().map_err(db_error)?;
        Ok(PutResult {
            e_tag: Some(etag.to_string()),
            version: None,
        })
    }

    fn copy_sync(&self, from: &str, to: &str, create: bool) -> object_store::Result<()> {
        let source = self.read(from)?;
        let attributes = decode_attributes(&source.attributes)?;
        self.put_sync(
            to.to_string(),
            source.body.into(),
            PutOptions {
                mode: if create {
                    PutMode::Create
                } else {
                    PutMode::Overwrite
                },
                attributes,
                ..PutOptions::default()
            },
        )?;
        Ok(())
    }
}

fn create_schema(connection: &Connection) -> object_store::Result<()> {
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(db_error)?;
    connection
        .execute_batch(
            "CREATE TABLE IF NOT EXISTS objects (
               key TEXT PRIMARY KEY,
               body BLOB NOT NULL,
               etag INTEGER NOT NULL,
               modified_ms INTEGER NOT NULL,
               attributes TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS store_sequence (
               singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
               next_etag INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO store_sequence(singleton, next_etag) VALUES (1, 1);",
        )
        .map_err(db_error)
}

fn connect(database: &FsPath) -> object_store::Result<Connection> {
    let connection = Connection::open(database).map_err(db_error)?;
    configure_connection(&connection)?;
    Ok(connection)
}

/// [`LocalStore::metadata`] in one of its databases.
fn metadata_in(database: &FsPath, prefix: &str) -> object_store::Result<Vec<StoredObject>> {
    let connection = connect(database)?;
    let mut statement = connection
        .prepare(
            "SELECT key, CAST(X'' AS BLOB), etag, modified_ms, attributes, length(body)
             FROM objects WHERE key >= ?1 ORDER BY key",
        )
        .map_err(db_error)?;
    let rows = statement
        .query_map([prefix], |row| {
            Ok(StoredObject {
                key: row.get(0)?,
                body: row.get(1)?,
                size: row.get(5)?,
                etag: row.get(2)?,
                modified_ms: row.get(3)?,
                attributes: row.get(4)?,
            })
        })
        .map_err(db_error)?;
    let mut objects = Vec::new();
    for row in rows {
        let object = row.map_err(db_error)?;
        if !object.key.starts_with(prefix) {
            break;
        }
        objects.push(object);
    }
    Ok(objects)
}

fn configure_connection(connection: &Connection) -> object_store::Result<()> {
    // `synchronous` is connection-local. Setting it only while the schema is
    // created leaves later writes at the bundled SQLite default, so a build
    // configuration can silently weaken the development store's durability.
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(db_error)?;
    connection
        .busy_timeout(Duration::from_secs(30))
        .map_err(db_error)
}

impl fmt::Display for LocalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LocalStore({})", self.database.display())
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let store = self.clone();
        let key = location.to_string();
        let body: Bytes = payload.into();
        crate::asyncrt::blocking(move || store.put_sync(key, body, options))
            .await
            .map_err(db_error)?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(LocalUpload {
            store: self.clone(),
            location: location.clone(),
            attributes: options.attributes,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self.clone();
        let key = location.to_string();
        let object = crate::asyncrt::blocking(move || store.read(&key))
            .await
            .map_err(db_error)??;
        let meta = object_meta(&object)?;
        options.check_preconditions(&meta)?;
        let full = 0..object.body.len() as u64;
        let range = match options.range {
            Some(range) => range.as_range(full.end).map_err(db_error)?,
            None => full,
        };
        let body = Bytes::from(object.body).slice(range.start as usize..range.end as usize);
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: decode_attributes(&object.attributes)?,
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let key = location.to_string();
        crate::asyncrt::blocking(move || {
            connect(store.database_of(&key))?
                .execute("DELETE FROM objects WHERE key = ?1", [&key])
                .map_err(db_error)?;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let scan = prefix.as_ref().map_or("", Path::as_ref);
        let result = self.metadata(scan).and_then(|objects| {
            objects
                .into_iter()
                .map(|object| object_meta(&object))
                .filter(|meta| {
                    meta.as_ref().map_or(true, |meta| {
                        prefix.as_ref().is_none_or(|prefix| {
                            meta.location
                                .prefix_match(prefix)
                                .is_some_and(|mut remainder| remainder.next().is_some())
                        })
                    })
                })
                .collect::<object_store::Result<Vec<_>>>()
        });
        match result {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for object in self.metadata(prefix.as_ref())? {
            let location = stored_location(&object.key)?;
            let Some(mut remainder) = location.prefix_match(&prefix) else {
                continue;
            };
            let Some(child) = remainder.next() else {
                continue;
            };
            if remainder.next().is_some() {
                common_prefixes.insert(prefix.child(child));
            } else {
                objects.push(object_meta(&object)?);
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, false))
            .await
            .map_err(db_error)?
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, true))
            .await
            .map_err(db_error)?
    }
}

#[async_trait]
impl PaginatedListStore for LocalStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let delimiter = options.delimiter.as_deref();
        let limit = options.max_keys.unwrap_or(usize::MAX);
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for object in self.metadata(prefix)? {
            let Some(remainder) = object.key.strip_prefix(prefix) else {
                continue;
            };
            if after.is_some_and(|after| object.key.as_str() <= after) {
                continue;
            }
            let common = delimiter.and_then(|delimiter| {
                remainder.find(delimiter).map(|index| {
                    stored_location(&format!("{prefix}{}{}", &remainder[..index], delimiter))
                })
            });
            let common = common.transpose()?;
            if let Some(common) = common {
                if let Some((last, Some(previous), _)) = entries.last_mut() {
                    if *previous == common {
                        *last = object.key;
                        continue;
                    }
                }
                entries.push((object.key, Some(common), None));
            } else {
                entries.push((object.key.clone(), None, Some(object_meta(&object)?)));
            }
        }
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let page_token = truncated
            .then(|| entries.last().map(|(last, _, _)| last.clone()))
            .flatten();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };
        for (_, common, object) in entries {
            if let Some(common) = common {
                result.common_prefixes.push(common);
            }
            if let Some(object) = object {
                result.objects.push(object);
            }
        }
        Ok(PaginatedListResult { result, page_token })
    }
}

#[derive(Debug)]
struct LocalUpload {
    store: LocalStore,
    location: Path,
    attributes: Attributes,
    parts: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> BoxFuture<'static, object_store::Result<()>> {
        self.parts.lock().unwrap().push(data.into());
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = std::mem::take(&mut *self.parts.lock().unwrap());
        let bytes = parts.iter().map(Bytes::len).sum();
        let mut body = Vec::with_capacity(bytes);
        for part in parts {
            body.extend_from_slice(&part);
        }
        self.store
            .put_opts(
                &self.location,
                body.into(),
                PutOptions {
                    attributes: self.attributes.clone(),
                    ..PutOptions::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

fn object_meta(object: &StoredObject) -> object_store::Result<ObjectMeta> {
    let modified = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_millis(object.modified_ms.max(0) as u64))
        .ok_or_else(|| message_error("the object timestamp is outside the system clock range"))?;
    Ok(ObjectMeta {
        location: stored_location(&object.key)?,
        last_modified: modified.into(),
        size: object.size,
        e_tag: Some(object.etag.to_string()),
        version: None,
    })
}

fn encode_attributes(attributes: &Attributes) -> object_store::Result<String> {
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
            _ => {
                return Err(message_error(
                    "the development store does not support this attribute",
                ))
            }
        };
        stored.push(StoredAttribute {
            kind: kind.to_string(),
            name,
            value: value.as_ref().to_string(),
        });
    }
    serde_json::to_string(&stored).map_err(db_error)
}

fn decode_attributes(encoded: &str) -> object_store::Result<Attributes> {
    let stored: Vec<StoredAttribute> = serde_json::from_str(encoded).map_err(db_error)?;
    let mut attributes = Attributes::with_capacity(stored.len());
    for stored in stored {
        let attribute = match stored.kind.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            "metadata" => Attribute::Metadata(stored.name.unwrap_or_default().into()),
            kind => return Err(message_error(format!("unknown stored attribute {kind:?}"))),
        };
        attributes.insert(attribute, AttributeValue::from(stored.value));
    }
    Ok(attributes)
}

#[cfg(all(test, celld_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_LOCAL_STORE_TESTS"));
}

/// The [`Path`] a stored key came from. Every key is stored as
/// `location.to_string()`, which is already percent-encoded, so this parses
/// that form back. `Path::from` would encode it a second time, and a listed
/// `přehled.html` would come back as `p%25C5%2599ehled.html`, a key that
/// names no object (denoland/celld#232).
fn stored_location(key: &str) -> object_store::Result<Path> {
    Ok(Path::parse(key)?)
}

fn db_error(error: impl fmt::Display) -> Error {
    message_error(error.to_string())
}

fn message_error(message: impl Into<String>) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn not_found(path: &str) -> Error {
    Error::NotFound {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the object does not exist",
        )),
    }
}

fn already_exists(path: &str) -> Error {
    Error::AlreadyExists {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the object already exists",
        )),
    }
}

fn precondition(path: &str) -> Error {
    Error::Precondition {
        path: path.to_string(),
        source: Box::new(std::io::Error::other("the ETag does not match")),
    }
}

#[cfg(test)]
mod prefix_tests {
    use super::*;
    use futures_util::TryStreamExt as _;

    /// A listing returns its prefix's objects and nothing else, whatever sorts
    /// on either side of the prefix: a sibling that shares its string, a key
    /// equal to it, and a key whose next character is U+10FFFF.
    #[tokio::test]
    async fn a_listing_returns_exactly_its_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalStore::open(directory.path().join("objects.sqlite3")).unwrap();
        let top = "cells/a/ltx/\u{10FFFF}";
        for key in [
            "cells/a/ltw/1",
            "cells/a/ltx",
            "cells/a/ltx/1",
            "cells/a/ltx/2",
            "cells/a/ltx/e1/3",
            top,
            "cells/a/ltxz/1",
            "cells/b/ltx/1",
        ] {
            store
                .put_sync(
                    key.to_string(),
                    Bytes::from_static(b"x"),
                    PutOptions::default(),
                )
                .unwrap();
        }
        let keys = |metas: &[ObjectMeta]| -> Vec<String> {
            metas.iter().map(|meta| meta.location.to_string()).collect()
        };

        let prefix = Path::from("cells/a/ltx");
        let listed: Vec<ObjectMeta> = store.list(Some(&prefix)).try_collect().await.unwrap();
        assert_eq!(
            keys(&listed),
            ["cells/a/ltx/1", "cells/a/ltx/2", "cells/a/ltx/e1/3", top]
        );

        let delimited = store.list_with_delimiter(Some(&prefix)).await.unwrap();
        assert_eq!(
            keys(&delimited.objects),
            ["cells/a/ltx/1", "cells/a/ltx/2", top]
        );
        assert_eq!(delimited.common_prefixes, [Path::from("cells/a/ltx/e1")]);

        let options = |page_token| PaginatedListOptions {
            max_keys: Some(2),
            page_token,
            ..PaginatedListOptions::default()
        };
        let first = store
            .list_paginated(Some("cells/a/ltx/"), options(None))
            .await
            .unwrap();
        assert_eq!(
            keys(&first.result.objects),
            ["cells/a/ltx/1", "cells/a/ltx/2"]
        );
        let second = store
            .list_paginated(Some("cells/a/ltx/"), options(first.page_token))
            .await
            .unwrap();
        assert_eq!(keys(&second.result.objects), ["cells/a/ltx/e1/3", top]);
        assert_eq!(second.page_token, None);
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;
    use crate::bucket::Bucket;

    /// The longest the core lets one renewal take (`actor::effects`: half
    /// the remaining authority, at most 2.5 s).
    const RENEWAL_BOUND: Duration = Duration::from_millis(2_500);

    const LEASE: &str = "nodes/dev-1.json";

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        crate::container::tests::rt().block_on(future)
    }

    /// A renewal does not wait for the node's other writes. SQLite admits one
    /// writer per database, and a write keeps it until its commit is on disk,
    /// on a busy disk for seconds. Here another write holds the objects'
    /// database past a renewal's bound. With the leases among the objects the
    /// renewal waited behind it and timed out, and five such misses in a row
    /// fenced the node (fragment's krun e2e, 2026-11-24).
    #[test]
    fn a_renewal_does_not_wait_for_another_write() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("objects.sqlite3");
        let bucket = Bucket::open_dev(&database).unwrap();
        let token = block_on(bucket.put_cas(LEASE, b"{\"expires_ms\":1}".to_vec(), None))
            .unwrap()
            .unwrap();

        // The node's LTX shipper, its write not yet committed.
        let mut other = Connection::open(&database).unwrap();
        let held = other
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        held.execute(
            "INSERT INTO objects(key, body, etag, modified_ms, attributes)
             VALUES ('cells/a/ltx/1', x'00', 0, 0, '[]')",
            [],
        )
        .unwrap();

        let renewed = block_on(crate::asyncrt::timeout(
            RENEWAL_BOUND,
            bucket.put_cas(LEASE, b"{\"expires_ms\":2}".to_vec(), Some(&token)),
        ));
        held.commit().unwrap();
        let renewed = renewed
            .expect("the renewal waited past its bound for another write to commit")
            .unwrap();
        assert!(renewed.is_some(), "the renewal's compare-and-swap was refused");
    }

    /// The leases keep their compare-and-swap across processes. Of two stores
    /// on one state directory (a node and the next one), a lease is created
    /// once, and of two renewals from one version, the second is refused.
    #[test]
    fn two_stores_change_a_lease_once() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("objects.sqlite3");
        let first = Bucket::open_dev(&database).unwrap();
        let second = Bucket::open_dev(&database).unwrap();

        let token = block_on(first.put_cas(LEASE, b"first".to_vec(), None))
            .unwrap()
            .unwrap();
        assert_eq!(
            block_on(second.put_cas(LEASE, b"second".to_vec(), None)).unwrap(),
            None
        );
        assert!(block_on(first.put_cas(LEASE, b"renewed".to_vec(), Some(&token)))
            .unwrap()
            .is_some());
        assert_eq!(
            block_on(second.put_cas(LEASE, b"stale".to_vec(), Some(&token))).unwrap(),
            None
        );
        let (body, _) = block_on(second.get(LEASE)).unwrap().unwrap();
        assert_eq!(&body[..], b"renewed");
    }

    /// A state directory from before the split keeps its leases: each moves
    /// to its own database with its version, so its holder's next renewal
    /// applies, and every version after it is new. The other objects stay,
    /// and a listing still reads both.
    #[test]
    fn a_store_from_before_the_split_keeps_its_leases() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("objects.sqlite3");
        {
            let connection = Connection::open(&database).unwrap();
            create_schema(&connection).unwrap();
            connection
                .execute_batch(
                    "INSERT INTO objects(key, body, etag, modified_ms, attributes)
                     VALUES ('nodes/dev-1.json', x'7b7d', 7, 0, '[]'),
                            ('cells/a/ltx/1', x'00', 8, 0, '[]');
                     UPDATE store_sequence SET next_etag = 9;",
                )
                .unwrap();
        }

        let bucket = Bucket::open_dev(&database).unwrap();
        let (body, token) = block_on(bucket.get(LEASE)).unwrap().unwrap();
        assert_eq!((&body[..], token.as_str()), (&b"{}"[..], "7"));
        let renewed = block_on(bucket.put_cas(LEASE, b"{}".to_vec(), Some("7")))
            .unwrap()
            .unwrap();
        assert!(renewed.parse::<i64>().unwrap() >= 9, "version {renewed} was issued before");

        let objects = Connection::open(&database).unwrap();
        let left: i64 = objects
            .query_row(
                "SELECT count(*) FROM objects WHERE key >= ?1 AND key < ?2",
                [LEASES, LEASES_END],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(left, 0, "a lease stayed among the objects");

        let store = LocalStore::open(&database).unwrap();
        let keys = |prefix| -> Vec<String> {
            store
                .metadata(prefix)
                .unwrap()
                .into_iter()
                .map(|object| object.key)
                .collect()
        };
        assert_eq!(keys(""), ["cells/a/ltx/1", LEASE]);
        assert_eq!(keys("nodes"), [LEASE]);
        assert_eq!(keys("cells/"), ["cells/a/ltx/1"]);
        let (_, token) = block_on(bucket.get(LEASE)).unwrap().unwrap();
        assert_eq!(token, renewed, "opening again changed the lease");
    }
}
