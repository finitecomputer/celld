// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! fork: the store behind the Workers Cache API (`caches.default`,
//! `caches.open(name)`), one per node.
//!
//! Cloudflare's Cache API is a view of the edge cache of the data center a
//! Worker runs in: best effort, never replicated, and free to drop an entry
//! at any time. A node is celld's data center, so each node keeps its own
//! cache in memory: another node never sees it, and a restart empties it.
//! Every isolate of the node shares it, so a response a Durable Object put
//! is the response a stateless request matches.
//!
//! What `put` keeps, and for how long, follows Cloudflare's documented rules
//! (`keep_for`). Two choices are stricter than Cloudflare's:
//!
//! - Entries are per script. Cloudflare shares a zone's cache among the
//!   zone's Workers; a node runs one deployment, and a script reading
//!   another script's entries is a leak nothing here needs.
//! - A Dynamic Worker (loaded through a Worker Loader) keeps nothing, and
//!   its `match` misses. Its code is a tenant's, and a node-wide store would
//!   let it fill the platform's cache or read what the platform kept.
//!
//! The store is bounded by bytes (`CELLD_CACHE_API_MAX_BYTES`) and by
//! entries (`MAX_ENTRIES`). It evicts expired entries first, then the least
//! recently used ones; each is found through an ordered index, so a call
//! costs a logarithm of the entries, not a walk of them.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

/// What the node keeps by default: enough for a page of operator numbers
/// or a few rendered pages, small beside an isolate's heap.
pub const MAX_BYTES_DEFAULT: u64 = 64 * 1024 * 1024;
/// The most entries the node keeps, whatever their size.
pub const MAX_ENTRIES: usize = 65_536;
/// The longest an entry lives, whatever its headers ask: a year, as
/// Cloudflare caps an edge TTL.
pub const MAX_TTL_MS: u64 = 365 * 24 * 60 * 60 * 1000;
/// The bytes an entry is charged beyond its strings and body: its record,
/// its key's one shared allocation and the three index nodes that point at
/// it. So a flood of empty responses still meets the byte bound.
const ENTRY_OVERHEAD_BYTES: u64 = 512;
/// The bytes each header is charged beyond its name and value: the pair's
/// two `String`s.
const HEADER_OVERHEAD_BYTES: u64 = std::mem::size_of::<(String, String)>() as u64;

/// One response as the cache keeps it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stored {
    pub status: u16,
    pub status_text: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Stored {
    /// What the entry costs the byte bound: every allocation it holds. The
    /// key is one allocation the indexes share.
    fn charge(&self, key: &Key) -> u64 {
        let headers: u64 = self
            .headers
            .iter()
            .map(|(n, v)| (n.len() + v.len()) as u64 + HEADER_OVERHEAD_BYTES)
            .sum();
        (self.body.len() + self.status_text.len() + key.charge()) as u64
            + headers
            + ENTRY_OVERHEAD_BYTES
    }
}

/// Which entry: the calling script, the cache (`None` for
/// `caches.default`, else the name given to `caches.open`), and the URL
/// without its fragment.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Key {
    pub script: String,
    pub cache: Option<String>,
    pub url: String,
}

impl Key {
    fn charge(&self) -> usize {
        self.script.len() + self.cache.as_deref().map_or(0, str::len) + self.url.len()
    }
}

/// Whether `put` keeps a response, and for how long.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keep {
    /// Kept this many milliseconds from now, with these headers (a field
    /// `Cache-Control: private=…` names, and `Cache-Tag`, dropped), and the
    /// age it arrived with (its `Age`), which a hit advances.
    For {
        ttl_ms: u64,
        headers: Vec<(String, String)>,
        age_s: Option<u64>,
    },
    /// Not kept, and why.
    Not(NotKept),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotKept {
    /// `Cache-Control: no-store`.
    NoStore,
    /// `Cache-Control: private` or `no-cache`, with no field named: the
    /// response is for one client, or must be revalidated, which the Cache
    /// API cannot do.
    Private,
    /// A `Cache-Control` value that does not parse (an unterminated quoted
    /// string): what it asks cannot be known, so nothing is kept.
    Malformed,
    /// A `Set-Cookie` header the response does not exclude with
    /// `Cache-Control: private=Set-Cookie`.
    SetCookie,
    /// A lifetime of zero: `max-age=0`, one its `Age` has used up, an
    /// `Expires` in the past or one that does not parse.
    Expired,
    /// No lifetime in the headers, and a status Cloudflare keeps none for by
    /// default.
    Status,
    /// A `304 Not Modified`: it validates a representation, and is none.
    NotModified,
}

impl NotKept {
    /// Whether a put that keeps nothing still empties the key: a later put
    /// says what the URL is now, unless it is a 304, which says the stored
    /// representation still holds.
    pub fn replaces(self) -> bool {
        self != NotKept::NotModified
    }
}

/// Cloudflare's rules for `cache.put`
/// (developers.cloudflare.com/workers/runtime-apis/cache/, "Headers"): the
/// response's `Cache-Control` (`s-maxage` before `max-age`, less its `Age`;
/// `no-store`, `private` and `no-cache` keep nothing), then `Expires`, then
/// the default edge TTL of its status (200 and 301 for two hours, 302 and 303
/// for twenty minutes, 404 and 410 for three); a `Set-Cookie` keeps nothing
/// unless `private=Set-Cookie` drops it; `Cache-Tag` is not returned by a
/// match. `stale-while-revalidate` and `stale-if-error` have no effect, as on
/// Cloudflare.
pub fn keep_for(status: u16, headers: &[(String, String)], now_wall_ms: i64) -> Keep {
    if status == 304 {
        return Keep::Not(NotKept::NotModified);
    }
    let mut control = Vec::new();
    for (_, value) in headers.iter().filter(|(name, _)| name.eq_ignore_ascii_case("cache-control")) {
        match directives(value) {
            Some(parsed) => control.extend(parsed),
            None => return Keep::Not(NotKept::Malformed),
        }
    }
    let mut dropped: Vec<String> = vec!["cache-tag".to_string()];
    for (name, value) in &control {
        match (name.as_str(), value) {
            ("no-store", _) => return Keep::Not(NotKept::NoStore),
            ("private" | "no-cache", None) => return Keep::Not(NotKept::Private),
            ("private" | "no-cache", Some(fields)) => dropped.extend(
                fields
                    .split(',')
                    .map(|field| field.trim().to_ascii_lowercase())
                    .filter(|field| !field.is_empty()),
            ),
            _ => {}
        }
    }
    let kept: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| !dropped.iter().any(|field| name.eq_ignore_ascii_case(field)))
        .cloned()
        .collect();
    if kept.iter().any(|(name, _)| name.eq_ignore_ascii_case("set-cookie")) {
        return Keep::Not(NotKept::SetCookie);
    }
    // an `Age` that does not parse is none (RFC 9111 5.1)
    let age_s = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("age"))
        .and_then(|(_, value)| value.trim().parse::<u64>().ok());
    let seconds = |directive: &str| {
        control
            .iter()
            .find(|(name, _)| name == directive)
            // a lifetime that does not parse is no lifetime (RFC 9111 4.2.1)
            .map(|(_, value)| value.as_deref().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0))
    };
    let ttl_ms = match seconds("s-maxage").or_else(|| seconds("max-age")) {
        // what is left of its lifetime: the time it spent elsewhere is spent
        Some(seconds) => seconds.saturating_sub(age_s.unwrap_or(0)).saturating_mul(1000),
        None => match headers.iter().find(|(name, _)| name.eq_ignore_ascii_case("expires")) {
            Some((_, value)) => match httpdate::parse_http_date(value.trim()) {
                Ok(at) => {
                    let at_ms = at
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis().min(i64::MAX as u128) as i64);
                    at_ms.saturating_sub(now_wall_ms).max(0) as u64
                }
                Err(_) => 0,
            },
            None => match status {
                200 | 206 | 301 => 120 * 60 * 1000,
                302 | 303 => 20 * 60 * 1000,
                404 | 410 => 3 * 60 * 1000,
                _ => return Keep::Not(NotKept::Status),
            },
        },
    };
    if ttl_ms == 0 {
        return Keep::Not(NotKept::Expired);
    }
    Keep::For {
        ttl_ms: ttl_ms.min(MAX_TTL_MS),
        headers: kept,
        age_s,
    }
}

/// A `Cache-Control` value's directives, each a lowercase name and its
/// value, unquoted. A comma inside a quoted string does not split, and a
/// backslash in one escapes the next character (RFC 9110 5.6.4); `None`
/// when a quoted string does not end.
fn directives(value: &str) -> Option<Vec<(String, Option<String>)>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' if quoted => current.push(chars.next()?),
            '"' => quoted = !quoted,
            ',' if !quoted => parts.push(std::mem::take(&mut current)),
            _ => current.push(c),
        }
    }
    if quoted {
        return None;
    }
    parts.push(current);
    Some(
        parts
            .into_iter()
            .filter_map(|part| {
                let part = part.trim();
                if part.is_empty() {
                    return None;
                }
                Some(match part.split_once('=') {
                    Some((name, value)) => {
                        (name.trim().to_ascii_lowercase(), Some(value.trim().to_string()))
                    }
                    None => (part.to_ascii_lowercase(), None),
                })
            })
            .collect(),
    )
}

struct Entry {
    stored: Arc<Stored>,
    expires_at_ms: u64,
    put_at_ms: u64,
    age_s: Option<u64>,
    charge: u64,
    /// Its place in the use order, renewed by each hit.
    used: u64,
    /// Its place in the expiry order, fixed at put.
    id: u64,
}

/// The node's entries, bounded by bytes and by count.
pub struct Store {
    max_bytes: u64,
    bytes: u64,
    entries: HashMap<Arc<Key>, Entry>,
    /// The entries by last use, oldest first: what eviction takes.
    order: BTreeMap<u64, Arc<Key>>,
    /// The entries by when they expire, soonest first.
    expiry: BTreeMap<(u64, u64), Arc<Key>>,
    tick: u64,
}

/// A hit: the response, and its age now (when it arrived with one).
pub struct Hit {
    pub stored: Arc<Stored>,
    pub age_s: Option<u64>,
}

impl Store {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            bytes: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
            expiry: BTreeMap::new(),
            tick: 0,
        }
    }

    /// Keep `stored` under `key` until `now_ms + ttl_ms` (a monotonic
    /// clock), replacing what the key held. One larger than the whole store
    /// is not kept, and the key then holds nothing: a later put says what
    /// the URL is now. Answers whether it was kept.
    pub fn put(&mut self, key: Key, stored: Stored, ttl_ms: u64, age_s: Option<u64>, now_ms: u64) -> bool {
        self.remove(&key);
        let charge = stored.charge(&key);
        if charge > self.max_bytes {
            return false;
        }
        self.evict_expired(now_ms);
        while self.bytes + charge > self.max_bytes || self.entries.len() >= MAX_ENTRIES {
            let Some(oldest) = self.order.first_key_value().map(|(_, key)| key.clone()) else {
                break;
            };
            self.remove(&oldest);
        }
        self.tick += 1;
        let key = Arc::new(key);
        let expires_at_ms = now_ms.saturating_add(ttl_ms);
        self.order.insert(self.tick, key.clone());
        self.expiry.insert((expires_at_ms, self.tick), key.clone());
        self.bytes += charge;
        self.entries.insert(
            key,
            Entry {
                stored: Arc::new(stored),
                expires_at_ms,
                put_at_ms: now_ms,
                age_s,
                charge,
                used: self.tick,
                id: self.tick,
            },
        );
        debug_assert!(self.entries.len() == self.order.len() && self.entries.len() == self.expiry.len());
        true
    }

    /// The response `key` holds, unless it has expired.
    pub fn get(&mut self, key: &Key, now_ms: u64) -> Option<Hit> {
        let entry = self.entries.get(key)?;
        if entry.expires_at_ms <= now_ms {
            self.remove(key);
            return None;
        }
        let used = entry.used;
        let hit = Hit {
            stored: entry.stored.clone(),
            age_s: entry
                .age_s
                .map(|age| age.saturating_add(now_ms.saturating_sub(entry.put_at_ms) / 1000)),
        };
        self.tick += 1;
        if let Some(shared) = self.order.remove(&used) {
            self.order.insert(self.tick, shared);
        }
        if let Some(entry) = self.entries.get_mut(key) {
            entry.used = self.tick;
        }
        Some(hit)
    }

    /// Whether `key` held a response that had not expired; it holds none
    /// after.
    pub fn delete(&mut self, key: &Key, now_ms: u64) -> bool {
        match self.remove(key) {
            Some(entry) => entry.expires_at_ms > now_ms,
            None => false,
        }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&mut self, key: &Key) -> Option<Entry> {
        let entry = self.entries.remove(key)?;
        self.order.remove(&entry.used);
        self.expiry.remove(&(entry.expires_at_ms, entry.id));
        self.bytes -= entry.charge;
        Some(entry)
    }

    /// Let go of every entry expired by `now_ms`: the front of the expiry
    /// order, and only it.
    fn evict_expired(&mut self, now_ms: u64) {
        while let Some(((expires_at_ms, _), key)) = self.expiry.first_key_value() {
            if *expires_at_ms > now_ms {
                break;
            }
            let key = key.clone();
            self.remove(&key);
        }
    }
}

/// The node's store, sized by `CELLD_CACHE_API_MAX_BYTES` (validated at
/// start: `env_vars::validate`; 0 keeps nothing).
pub fn node() -> &'static Mutex<Store> {
    static STORE: OnceLock<Mutex<Store>> = OnceLock::new();
    STORE.get_or_init(|| {
        let max = crate::env_vars::with_default("CELLD_CACHE_API_MAX_BYTES", MAX_BYTES_DEFAULT)
            .expect("validated CELLD_CACHE_API_MAX_BYTES");
        Mutex::new(Store::new(max))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(n, v)| (n.to_string(), v.to_string())).collect()
    }

    fn ttl(status: u16, headers: &[(&str, &str)]) -> Keep {
        keep_for(status, &h(headers), 1_000_000_000_000)
    }

    fn kept_ms(keep: &Keep) -> Option<u64> {
        match keep {
            Keep::For { ttl_ms, .. } => Some(*ttl_ms),
            Keep::Not(_) => None,
        }
    }

    // Goal: a response is kept as long as Cloudflare's rules say, in their
    // order: s-maxage, max-age (less its Age), Expires, the status's
    // default; and not at all for no-store, private, no-cache, a cookie, a
    // zero lifetime, a 304, a Cache-Control that does not parse, or a status
    // with no default. Replay: asked twice, the same.
    #[test]
    fn a_response_is_kept_as_cloudflare_keeps_it() {
        for _ in 0..2 {
            assert_eq!(kept_ms(&ttl(200, &[("Cache-Control", "max-age=120")])), Some(120_000));
            assert_eq!(kept_ms(&ttl(200, &[("cache-control", "public, max-age=60, s-maxage=30")])), Some(30_000));
            assert_eq!(kept_ms(&ttl(500, &[("cache-control", "max-age=5")])), Some(5_000), "any status, given a lifetime");
            assert_eq!(kept_ms(&ttl(200, &[])), Some(120 * 60_000));
            assert_eq!(kept_ms(&ttl(301, &[])), Some(120 * 60_000));
            assert_eq!(kept_ms(&ttl(302, &[])), Some(20 * 60_000));
            assert_eq!(kept_ms(&ttl(404, &[])), Some(3 * 60_000));
            assert_eq!(kept_ms(&ttl(200, &[("cache-control", "max-age=99999999999")])), Some(MAX_TTL_MS));
            assert_eq!(kept_ms(&ttl(200, &[("cache-control", "max-age=60, stale-while-revalidate=600")])), Some(60_000));
            assert_eq!(kept_ms(&ttl(200, &[("cache-control", "max-age=60"), ("age", "55")])), Some(5_000), "what its Age left");
            assert_eq!(kept_ms(&ttl(200, &[("cache-control", "max-age=60"), ("age", "soon")])), Some(60_000), "an Age that does not parse is none");
            // Expires, against the clock: 1_000_000_000 s is 2001-09-09T01:46:40Z
            assert_eq!(kept_ms(&ttl(200, &[("Expires", "Sun, 09 Sep 2001 01:48:40 GMT")])), Some(120_000));
            assert_eq!(kept_ms(&ttl(200, &[("cache-control", "max-age=7"), ("expires", "Sun, 09 Sep 2001 01:48:40 GMT")])), Some(7_000), "max-age before Expires");

            assert_eq!(ttl(200, &[("cache-control", "no-store, max-age=60")]), Keep::Not(NotKept::NoStore));
            assert_eq!(ttl(200, &[("cache-control", "private, max-age=60")]), Keep::Not(NotKept::Private));
            assert_eq!(ttl(200, &[("cache-control", "no-cache")]), Keep::Not(NotKept::Private));
            assert_eq!(ttl(200, &[("cache-control", r#"ext="a\"b", private, max-age=60"#)]), Keep::Not(NotKept::Private), "an escaped quote hides nothing");
            assert_eq!(ttl(200, &[("cache-control", r#"ext="a, private"#)]), Keep::Not(NotKept::Malformed));
            assert_eq!(ttl(200, &[("set-cookie", "a=b"), ("cache-control", "max-age=60")]), Keep::Not(NotKept::SetCookie));
            assert_eq!(ttl(200, &[("cache-control", "max-age=0")]), Keep::Not(NotKept::Expired));
            assert_eq!(ttl(200, &[("cache-control", "max-age=60"), ("age", "60")]), Keep::Not(NotKept::Expired));
            assert_eq!(ttl(200, &[("cache-control", "max-age=soon")]), Keep::Not(NotKept::Expired), "a lifetime that does not parse is none");
            assert_eq!(ttl(200, &[("expires", "Sun, 09 Sep 2001 01:40:00 GMT")]), Keep::Not(NotKept::Expired));
            assert_eq!(ttl(200, &[("expires", "0")]), Keep::Not(NotKept::Expired));
            assert_eq!(ttl(304, &[("cache-control", "max-age=60")]), Keep::Not(NotKept::NotModified));
            assert_eq!(ttl(500, &[]), Keep::Not(NotKept::Status));
            assert_eq!(ttl(403, &[]), Keep::Not(NotKept::Status));
        }
        assert!(!NotKept::NotModified.replaces() && NotKept::NoStore.replaces() && NotKept::Status.replaces());
        // private=Set-Cookie keeps the response without its cookie; a
        // Cache-Tag is never returned
        let keep = ttl(200, &[("Set-Cookie", "a=b"), ("Cache-Control", "private=\"Set-Cookie, X-Mine\", max-age=60"), ("X-Mine", "1"), ("Cache-Tag", "t"), ("X-Kept", "2"), ("Age", "10")]);
        assert_eq!(
            keep,
            Keep::For {
                ttl_ms: 50_000,
                headers: h(&[("Cache-Control", "private=\"Set-Cookie, X-Mine\", max-age=60"), ("X-Kept", "2"), ("Age", "10")]),
                age_s: Some(10),
            }
        );
    }

    fn key(url: &str) -> Key {
        Key { script: "app".into(), cache: None, url: url.into() }
    }

    fn body(bytes: usize) -> Stored {
        Stored { status: 200, status_text: "OK".into(), headers: h(&[("content-type", "text/plain")]), body: vec![7; bytes] }
    }

    // Goal: an entry is answered until it expires, and not after, its Age
    // advanced by its time here; a put replaces it; a delete says whether it
    // held one; scripts and caches do not see each other's. Replay: a second
    // get answers the same.
    #[test]
    fn entries_live_until_they_expire() {
        let mut store = Store::new(1 << 20);
        assert!(store.put(key("https://a/1"), body(3), 10_000, Some(4), 10));
        for _ in 0..2 {
            let hit = store.get(&key("https://a/1"), 3_009).unwrap();
            assert_eq!((hit.stored.body.clone(), hit.age_s), (vec![7; 3], Some(6)), "its Age advanced");
        }
        assert!(store.get(&key("https://a/1"), 10_010).is_none(), "expired at its lifetime's end");
        assert!(store.is_empty() && store.bytes() == 0, "and let go");

        assert!(store.put(key("https://a/1"), body(3), 1_000, None, 0));
        assert!(store.put(key("https://a/1"), body(5), 1_000, None, 0));
        let hit = store.get(&key("https://a/1"), 1).unwrap();
        assert_eq!((hit.stored.body.len(), hit.age_s), (5, None), "a put replaces; no Age, none made up");
        assert_eq!(store.len(), 1);

        let other_script = Key { script: "other".into(), ..key("https://a/1") };
        let named = Key { cache: Some("mine".into()), ..key("https://a/1") };
        assert!(store.get(&other_script, 1).is_none(), "another script's");
        assert!(store.get(&named, 1).is_none(), "another cache's");

        assert!(store.delete(&key("https://a/1"), 1));
        assert!(!store.delete(&key("https://a/1"), 1), "nothing left to delete");
        assert!(store.put(key("https://a/2"), body(1), 5, None, 0));
        assert!(!store.delete(&key("https://a/2"), 5), "an expired one is not there to delete");
        assert_eq!((store.len(), store.bytes(), store.order.len(), store.expiry.len()), (0, 0, 0, 0));
    }

    // Goal: the store never holds more than its bound, every allocation of
    // an entry charged: a full store lets go of the expired, then the least
    // recently used; one entry larger than the whole store is not kept, and
    // the key it would replace holds nothing after.
    #[test]
    fn the_store_keeps_its_bound() {
        let one = body(1000).charge(&key("https://a/0"));
        assert!(one >= 1000 + 2 + 11 + 3 + 12 + HEADER_OVERHEAD_BYTES + ENTRY_OVERHEAD_BYTES, "body, status text, key, header and overheads");
        let mut store = Store::new(3 * one);
        for i in 0..3 {
            assert!(store.put(key(&format!("https://a/{i}")), body(1000), 1_000, None, 0));
        }
        assert_eq!(store.bytes(), 3 * one);
        assert!(store.get(&key("https://a/0"), 1).is_some(), "0 used again: 1 is now the oldest");
        assert!(store.put(key("https://a/3"), body(1000), 1_000, None, 2));
        assert!(store.get(&key("https://a/1"), 3).is_none(), "the least recently used let go");
        for i in [0, 2, 3] {
            assert!(store.get(&key(&format!("https://a/{i}")), 3).is_some(), "{i} kept");
        }
        assert!(store.bytes() <= 3 * one);

        // the expired go before any live one (keys of one length: one charge)
        let mut store = Store::new(3 * one);
        assert!(store.put(key("https://a/s"), body(1000), 5, None, 0));
        assert!(store.put(key("https://a/l"), body(1000), 1_000, None, 0));
        assert!(store.put(key("https://a/m"), body(1000), 1_000, None, 0));
        assert!(store.put(key("https://a/n"), body(1000), 1_000, None, 10));
        assert!(store.get(&key("https://a/s"), 11).is_none(), "the expired one went");
        assert!(store.get(&key("https://a/l"), 11).is_some() && store.get(&key("https://a/m"), 11).is_some(), "the live ones stayed");
        assert_eq!((store.order.len(), store.expiry.len()), (store.len(), store.len()));

        assert!(store.put(key("https://a/big"), body(10), 1_000, None, 0));
        assert!(!store.put(key("https://a/big"), body(4 * 1000), 1_000, None, 0), "larger than the store");
        assert!(store.get(&key("https://a/big"), 1).is_none(), "and the older one is gone");

        let mut none = Store::new(0);
        assert!(!none.put(key("https://a/0"), body(0), 1_000, None, 0), "a store of 0 bytes keeps nothing");
    }
}
