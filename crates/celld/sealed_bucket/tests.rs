use super::*;
use object_store::memory::InMemory;

const KEY: [u8; 32] = [1; 32];

/// The bucket, the counter's bucket, and a store over them.
struct World {
    bucket: Arc<InMemory>,
    counter: Arc<InMemory>,
}

impl World {
    fn new() -> World {
        World { bucket: Arc::new(InMemory::new()), counter: Arc::new(InMemory::new()) }
    }

    /// A process's store: a fresh one reads the bucket from its counter.
    fn store(&self) -> SealedStore {
        SealedStore::new(
            self.bucket.clone(),
            self.bucket.clone(),
            self.counter.clone(),
            Path::from("homes/test"),
            root_of("fleet/"),
            MasterKey::new(KEY),
        )
    }

    async fn raw(&self, name: &str) -> Bytes {
        self.bucket.get(&Path::from(name)).await.unwrap().bytes().await.unwrap()
    }

    async fn every_object(&self) -> Vec<(Path, Bytes)> {
        let metas: Vec<ObjectMeta> = self.bucket.list(None).try_collect().await.unwrap();
        let mut all = Vec::new();
        for meta in metas {
            all.push((meta.location.clone(), self.bucket.get(&meta.location).await.unwrap().bytes().await.unwrap()));
        }
        all
    }
}

fn sealed(error: &Error) -> Option<&SealError> {
    match error {
        Error::Generic { source, .. } => source.downcast_ref::<SealError>(),
        _ => None,
    }
}

fn p(name: &str) -> Path {
    Path::from(name)
}

async fn read(store: &SealedStore, name: &str) -> object_store::Result<Bytes> {
    store.get(&p(name)).await?.bytes().await
}

async fn counter_seq(world: &World) -> u64 {
    let store = world.store();
    store.read_counter().await.unwrap().map_or(0, |(record, _)| record.seq)
}

#[tokio::test]
async fn every_call_round_trips_and_the_bucket_holds_only_ciphertext() {
    let world = World::new();
    let store = world.store();
    let big: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
    let mut attributes = Attributes::new();
    attributes.insert(Attribute::ContentType, "text/plain".into());
    attributes.insert(Attribute::Metadata("celld-r2".into()), "{\"secret\":\"custom metadata\"}".into());
    store
        .put_opts(&p("fleet/r2/b/notes.txt"), PutPayload::from_static(b"a private note"), PutOptions { attributes: attributes.clone(), ..PutOptions::default() })
        .await
        .unwrap();
    store.put(&p("fleet/cells/a/ltx/1"), Bytes::from(big.clone()).into()).await.unwrap();
    store.put(&p("fleet/cells/a/ltx/e1/2"), PutPayload::from_static(b"")).await.unwrap();

    assert_eq!(read(&store, "fleet/r2/b/notes.txt").await.unwrap(), "a private note");
    let got = store.get(&p("fleet/r2/b/notes.txt")).await.unwrap();
    assert_eq!(got.attributes, attributes);
    assert_eq!(read(&store, "fleet/cells/a/ltx/1").await.unwrap(), big);
    assert_eq!(read(&store, "fleet/cells/a/ltx/e1/2").await.unwrap(), "");
    let range = store.get_range(&p("fleet/cells/a/ltx/1"), 65_530..131_080).await.unwrap();
    assert_eq!(range, big[65_530..131_080]);
    let suffix = store.get_opts(&p("fleet/cells/a/ltx/1"), GetOptions { range: Some(GetRange::Suffix(10)), ..GetOptions::default() }).await.unwrap();
    assert_eq!(suffix.range, 199_990..200_000);
    assert_eq!(suffix.bytes().await.unwrap(), big[199_990..]);
    let head = store.head(&p("fleet/cells/a/ltx/1")).await.unwrap();
    assert_eq!(head.size, 200_000);

    let listed: Vec<ObjectMeta> = store.list(Some(&p("fleet/cells/a/ltx"))).try_collect().await.unwrap();
    assert_eq!(listed.iter().map(|m| m.location.to_string()).collect::<Vec<_>>(), ["fleet/cells/a/ltx/1", "fleet/cells/a/ltx/e1/2"]);
    let delimited = store.list_with_delimiter(Some(&p("fleet/cells/a/ltx"))).await.unwrap();
    assert_eq!(delimited.objects.len(), 1);
    assert_eq!(delimited.common_prefixes, [p("fleet/cells/a/ltx/e1")]);
    let page = store
        .list_paginated(Some("fleet/"), PaginatedListOptions { max_keys: Some(2), ..PaginatedListOptions::default() })
        .await
        .unwrap();
    assert_eq!(page.result.objects.len(), 2);
    let rest = store
        .list_paginated(Some("fleet/"), PaginatedListOptions { max_keys: Some(2), page_token: page.page_token, ..PaginatedListOptions::default() })
        .await
        .unwrap();
    assert_eq!(rest.result.objects.iter().map(|m| m.location.to_string()).collect::<Vec<_>>(), ["fleet/r2/b/notes.txt"]);
    assert_eq!(rest.page_token, None);

    store.copy(&p("fleet/r2/b/notes.txt"), &p("fleet/r2/b/copy.txt")).await.unwrap();
    assert_eq!(read(&store, "fleet/r2/b/copy.txt").await.unwrap(), "a private note");
    assert!(matches!(store.copy_if_not_exists(&p("fleet/r2/b/notes.txt"), &p("fleet/r2/b/copy.txt")).await, Err(Error::AlreadyExists { .. })));
    store.delete(&p("fleet/r2/b/copy.txt")).await.unwrap();
    assert!(matches!(store.get(&p("fleet/r2/b/copy.txt")).await, Err(Error::NotFound { .. })));
    store.delete(&p("fleet/r2/b/never")).await.unwrap();

    let mut upload = store.put_multipart(&p("fleet/r2/b/big")).await.unwrap();
    let parts: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 6 << 20]).collect();
    for part in &parts {
        upload.put_part(part.clone().into()).await.unwrap();
    }
    upload.complete().await.unwrap();
    assert_eq!(read(&store, "fleet/r2/b/big").await.unwrap(), parts.concat());

    // Nothing the store was given is in the bucket in the clear.
    for (path, bytes) in world.every_object().await {
        for needle in [b"a private note".as_slice(), b"custom metadata", b"text/plain"] {
            assert!(!bytes.windows(needle.len()).any(|w| w == needle), "{path} holds plaintext");
        }
        let meta = world.bucket.get(&path).await.unwrap();
        assert!(meta.attributes.is_empty(), "{path} carries attributes in the clear");
    }
}

#[tokio::test]
async fn a_restart_reads_the_last_commit_from_the_counter() {
    let world = World::new();
    {
        let store = world.store();
        for i in 0..10 {
            store.put(&p(&format!("fleet/cells/c{i}/own.json")), Bytes::from(format!("owner {i}")).into()).await.unwrap();
        }
        store.delete(&p("fleet/cells/c3/own.json")).await.unwrap();
    }
    let store = world.store();
    let listed: Vec<ObjectMeta> = store.list(Some(&p("fleet/cells"))).try_collect().await.unwrap();
    assert_eq!(listed.len(), 9);
    assert_eq!(read(&store, "fleet/cells/c7/own.json").await.unwrap(), "owner 7");
    assert!(matches!(store.get(&p("fleet/cells/c3/own.json")).await, Err(Error::NotFound { .. })));
    store.put(&p("fleet/cells/c3/own.json"), PutPayload::from_static(b"again")).await.unwrap();
    assert_eq!(read(&world.store(), "fleet/cells/c3/own.json").await.unwrap(), "again");
}

#[tokio::test]
async fn conditional_writes_keep_their_meaning() {
    let world = World::new();
    let store = world.store();
    let name = p("fleet/cells/a/own.json");
    let first = store.put_opts(&name, PutPayload::from_static(b"1"), PutMode::Create.into()).await.unwrap();
    assert!(matches!(store.put_opts(&name, PutPayload::from_static(b"2"), PutMode::Create.into()).await, Err(Error::AlreadyExists { .. })));
    let stale = UpdateVersion { e_tag: Some("not it".into()), version: None };
    assert!(matches!(store.put_opts(&name, PutPayload::from_static(b"2"), PutMode::Update(stale).into()).await, Err(Error::Precondition { .. })));
    let current = UpdateVersion { e_tag: first.e_tag.clone(), version: first.version.clone() };
    let second = store.put_opts(&name, PutPayload::from_static(b"2"), PutMode::Update(current.clone()).into()).await.unwrap();
    assert_ne!(second.e_tag, first.e_tag);
    assert!(matches!(store.put_opts(&name, PutPayload::from_static(b"3"), PutMode::Update(current).into()).await, Err(Error::Precondition { .. })));
    let unmet = GetOptions { if_match: first.e_tag.clone(), ..GetOptions::default() };
    assert!(matches!(store.get_opts(&name, unmet).await, Err(Error::Precondition { .. })));
    assert_eq!(read(&store, "fleet/cells/a/own.json").await.unwrap(), "2");
}

#[tokio::test]
async fn an_older_version_served_by_the_bucket_is_refused() {
    let world = World::new();
    let store = world.store();
    let name = "fleet/cells/a/own.json";
    store.put(&p(name), PutPayload::from_static(b"epoch 1")).await.unwrap();
    let older = world.raw(name).await;
    store.put(&p(name), PutPayload::from_static(b"epoch 2")).await.unwrap();
    // Whoever holds the bucket puts the older object back.
    world.bucket.put(&p(name), older.into()).await.unwrap();
    let refused = read(&store, name).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::Tampered { .. })), "{refused}");
    // And a restart believes the counter, not the bucket.
    let refused = read(&world.store(), name).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::Tampered { .. })), "{refused}");
}

#[tokio::test]
async fn a_changed_byte_is_refused() {
    let world = World::new();
    let store = world.store();
    let name = "fleet/r2/b/k";
    store.put(&p(name), PutPayload::from_static(b"some bytes")).await.unwrap();
    let mut bytes = world.raw(name).await.to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    world.bucket.put(&p(name), bytes.into()).await.unwrap();
    // A fresh process, whose index is the counter's (its ETag is the old one).
    let fresh = world.store();
    let refused = read(&fresh, name).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::Tampered { .. })), "{refused}");
}

#[tokio::test]
async fn a_hidden_commit_refuses_the_start() {
    let world = World::new();
    let store = world.store();
    for i in 0..4 {
        store.put(&p(&format!("fleet/k{i}")), PutPayload::from_static(b"x")).await.unwrap();
    }
    assert_eq!(counter_seq(&world).await, 4);
    world.bucket.delete(&store.journal_path(2)).await.unwrap();
    let refused = world.store().head(&p("fleet/k0")).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::History(_))), "{refused}");
}

#[tokio::test]
async fn a_bucket_rolled_back_whole_is_refused() {
    let world = World::new();
    let store = world.store();
    store.put(&p("fleet/k"), PutPayload::from_static(b"1")).await.unwrap();
    let then = world.every_object().await;
    store.put(&p("fleet/k"), PutPayload::from_static(b"2")).await.unwrap();
    store.put(&p("fleet/j"), PutPayload::from_static(b"3")).await.unwrap();
    // The whole bucket as it was, every object of it, and nothing newer.
    let now: Vec<ObjectMeta> = world.bucket.list(None).try_collect().await.unwrap();
    for meta in now {
        world.bucket.delete(&meta.location).await.unwrap();
    }
    for (path, bytes) in then {
        world.bucket.put(&path, bytes.into()).await.unwrap();
    }
    let refused = world.store().head(&p("fleet/k")).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::History(_))), "{refused}");
}

#[tokio::test]
async fn a_second_writer_never_wins() {
    let world = World::new();
    world.store().put(&p("fleet/base"), PutPayload::from_static(b"0")).await.unwrap();
    let a = world.store();
    let b = world.store();
    // Both have read the bucket at the same commit.
    a.head(&p("fleet/base")).await.unwrap();
    b.head(&p("fleet/base")).await.unwrap();
    a.put(&p("fleet/a"), PutPayload::from_static(b"a")).await.unwrap();
    let fenced = b.put(&p("fleet/b"), PutPayload::from_static(b"b")).await.unwrap_err();
    assert!(matches!(sealed(&fenced), Some(SealError::Fenced(_))), "{fenced}");
    // Fenced for good: not even a read, which would be stale.
    assert!(matches!(sealed(&b.head(&p("fleet/a")).await.unwrap_err()), Some(SealError::Fenced(_))));
    // The first writer goes on, and a fresh process sees its writes alone.
    a.put(&p("fleet/a2"), PutPayload::from_static(b"a2")).await.unwrap();
    let fresh = world.store();
    assert_eq!(read(&fresh, "fleet/a").await.unwrap(), "a");
    assert!(matches!(fresh.get(&p("fleet/b")).await, Err(Error::NotFound { .. })));

    // A second writer to the same object is stopped at the bucket.
    let c = world.store();
    c.head(&p("fleet/a")).await.unwrap();
    fresh.put(&p("fleet/a"), PutPayload::from_static(b"fresh")).await.unwrap();
    let refused = c.put(&p("fleet/a"), PutPayload::from_static(b"c")).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::Tampered { .. })), "{refused}");
    assert_eq!(read(&world.store(), "fleet/a").await.unwrap(), "fresh");
}

#[tokio::test]
async fn a_bucket_with_history_and_no_counter_is_refused() {
    let world = World::new();
    world.store().put(&p("fleet/k"), PutPayload::from_static(b"1")).await.unwrap();
    world.counter.delete(&p("homes/test")).await.unwrap();
    let refused = world.store().head(&p("fleet/k")).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::History(_))), "{refused}");
}

#[tokio::test]
async fn another_key_opens_nothing() {
    let world = World::new();
    world.store().put(&p("fleet/k"), PutPayload::from_static(b"1")).await.unwrap();
    let other = SealedStore::new(world.bucket.clone(), world.bucket.clone(), world.counter.clone(), Path::from("homes/test"), root_of("fleet/"), MasterKey::new([2; 32]));
    let refused = other.head(&p("fleet/k")).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::History(_))), "{refused}");
}

#[tokio::test]
async fn snapshots_bound_a_start_and_drop_the_old_journal() {
    let world = World::new();
    let store = world.store();
    let commits = SNAPSHOT_EVERY * 2 + 5;
    for i in 0..commits {
        store.put(&p(&format!("fleet/k{}", i % 7)), Bytes::from(format!("v{i}")).into()).await.unwrap();
    }
    let snapshots: Vec<ObjectMeta> = world.bucket.list(Some(&root_of("fleet/").child("snapshot"))).try_collect().await.unwrap();
    assert_eq!(snapshots.len(), 2);
    let journal: Vec<ObjectMeta> = world.bucket.list(Some(&root_of("fleet/").child("journal"))).try_collect().await.unwrap();
    assert!(journal.len() as u64 <= SNAPSHOT_EVERY + 5, "{} journal records left", journal.len());
    let fresh = world.store();
    for k in 0..7 {
        let last = (0..commits).filter(|i| i % 7 == k).last().unwrap();
        assert_eq!(read(&fresh, &format!("fleet/k{k}")).await.unwrap(), format!("v{last}"));
    }
}

#[tokio::test]
async fn concurrent_writes_share_commits() {
    let world = World::new();
    let store = world.store();
    store.head(&p("fleet/none")).await.unwrap_err();
    let writes = (0..64).map(|i| {
        let store = store.clone();
        async move { store.put(&p(&format!("fleet/w{i}")), Bytes::from(format!("{i}")).into()).await }
    });
    for result in futures_util::future::join_all(writes).await {
        result.unwrap();
    }
    let seq = counter_seq(&world).await;
    assert!(seq <= 64, "{seq} commits");
    let fresh = world.store();
    for i in 0..64 {
        assert_eq!(read(&fresh, &format!("fleet/w{i}")).await.unwrap(), format!("{i}"));
    }
}

#[tokio::test]
async fn its_own_names_are_not_celld_s() {
    let world = World::new();
    let store = world.store();
    let refused = store.put(&p("fleet/sealed-bucket/journal/0000000000000001"), PutPayload::from_static(b"x")).await.unwrap_err();
    assert!(matches!(sealed(&refused), Some(SealError::Refused(_))), "{refused}");
}

#[test]
fn a_configuration_is_checked_whole() {
    let good = br#"{"key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE","counter":{"bucket":"s3://c","name":"homes/h","region":"r","access_key_id":"a","secret_access_key":"s"}}"#;
    let config = Config::parse(good).unwrap();
    assert_eq!(config.counter.name, "homes/h");
    assert!(!format!("{:?}", config.counter).contains("\"s\""), "a secret in Debug");
    let short = br#"{"key":"AQE","counter":{"bucket":"s3://c","name":"h","region":"r","access_key_id":"a","secret_access_key":"s"}}"#;
    assert!(Config::parse(short).is_err());
    let not_s3 = br#"{"key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE","counter":{"bucket":"gs://c","name":"h","region":"r","access_key_id":"a","secret_access_key":"s"}}"#;
    assert!(Config::parse(not_s3).is_err());
    let extra = br#"{"key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE","more":1,"counter":{"bucket":"s3://c","name":"h","region":"r","access_key_id":"a","secret_access_key":"s"}}"#;
    assert!(Config::parse(extra).is_err());
}
