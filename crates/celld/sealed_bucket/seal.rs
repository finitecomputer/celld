//! The sealed bucket's cryptography, pure: how one object is sealed, how the
//! store's own records are, and the chain its commits make
//! (docs/services/sealed-bucket.md).
//!
//! An object is a header (`OBJECT_MAGIC`, then the object's salt) and its
//! plaintext cut into `CHUNK_BYTES` chunks, each sealed with AES-256-GCM
//! under a key of its own: HKDF of the bucket's key, the object's name and
//! its salt, so an object opens only under its own name and only in the
//! version the store's index names. A chunk's nonce is its index, and its
//! additional data says whether it is the last, so a reordered, dropped or
//! truncated chunk does not open.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine as _;
use sha2::{Digest, Sha256};
use std::ops::Range;

pub const KEY_BYTES: usize = 32;
pub const SALT_BYTES: usize = 32;
pub const OBJECT_MAGIC: &[u8; 8] = b"celldsb1";
pub const HEADER_BYTES: u64 = OBJECT_MAGIC.len() as u64 + SALT_BYTES as u64;
/// One chunk's plaintext: a range read fetches whole chunks.
pub const CHUNK_BYTES: u64 = 64 * 1024;
pub const TAG_BYTES: u64 = 16;
pub const SEALED_CHUNK_BYTES: u64 = CHUNK_BYTES + TAG_BYTES;
/// An object, at most: S3's own limit (5 TiB).
pub const OBJECT_BYTES_MAX: u64 = 5 << 40;
pub const RECORD_MAGIC: &[u8; 8] = b"celldsr1";
const NONCE_BYTES: usize = 12;
const RECORD_HEADER_BYTES: usize = RECORD_MAGIC.len() + NONCE_BYTES;
/// HKDF's salt: a key of another purpose never derives these.
const KDF_SALT: &[u8] = b"celld sealed bucket v1";
const CHAIN_LABEL: &[u8] = b"celld sealed bucket chain v1";

const _: () = assert!(CHUNK_BYTES.is_power_of_two() && CHUNK_BYTES >= 4096);
const _: () = assert!(OBJECT_BYTES_MAX / CHUNK_BYTES < u32::MAX as u64, "a chunk index fits in its nonce");

/// Why sealed bytes did not open. Each says only what failed: never a key,
/// a plaintext or a name's bytes beyond the caller's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// A chunk that is not this object's, in this version, at this place.
    Chunk { index: u64 },
    /// Bytes that are no sealed object's header.
    Header,
    /// Fewer or more chunks than the object's size says.
    Length,
    /// A record that is not this store's of this kind.
    Record { kind: &'static str },
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refused::Chunk { index } => write!(f, "chunk {index} does not open: not this object's version"),
            Refused::Header => f.write_str("not a sealed object's header"),
            Refused::Length => f.write_str("the sealed object is not as long as its size says"),
            Refused::Record { kind } => write!(f, "the {kind} record does not open under this bucket's key"),
        }
    }
}

impl std::error::Error for Refused {}

/// The bucket's key. Its `Debug` prints nothing of it.
pub struct MasterKey([u8; KEY_BYTES]);

impl std::fmt::Debug for MasterKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MasterKey(..)")
    }
}

impl MasterKey {
    pub fn new(bytes: [u8; KEY_BYTES]) -> Self {
        Self(bytes)
    }

    fn derive(&self, info: &[&[u8]]) -> Aes256Gcm {
        let info: Vec<u8> = info.concat();
        let mut okm = [0u8; KEY_BYTES];
        hkdf::Hkdf::<Sha256>::new(Some(KDF_SALT), &self.0)
            .expand(&info, &mut okm)
            .expect("32 bytes is a valid HKDF-SHA256 length");
        Aes256Gcm::new_from_slice(&okm).expect("a 32-byte key")
    }

    /// The key of `name`'s object in the version whose salt is `salt`.
    pub fn object(&self, name: &str, salt: &[u8; SALT_BYTES]) -> ObjectKey {
        ObjectKey(self.derive(&[b"object\0", salt, name.as_bytes()]))
    }

    /// The key of the store's own records: its journal, snapshots and counter.
    pub fn records(&self) -> RecordKey {
        RecordKey(self.derive(&[b"records\0"]))
    }
}

pub struct ObjectKey(Aes256Gcm);

/// `index`'s nonce: unique under the object's own key.
fn chunk_nonce(index: u64) -> [u8; NONCE_BYTES] {
    let mut nonce = [0u8; NONCE_BYTES];
    nonce[4..].copy_from_slice(&index.to_be_bytes());
    nonce
}

fn chunk_aad(index: u64, last: bool) -> [u8; 17] {
    let mut aad = [0u8; 17];
    aad[..8].copy_from_slice(OBJECT_MAGIC);
    aad[8..16].copy_from_slice(&index.to_be_bytes());
    aad[16] = u8::from(last);
    aad
}

impl ObjectKey {
    pub fn seal_chunk(&self, index: u64, last: bool, plain: &[u8]) -> Vec<u8> {
        assert!(plain.len() as u64 <= CHUNK_BYTES, "a chunk is at most {CHUNK_BYTES} bytes");
        assert!(last || plain.len() as u64 == CHUNK_BYTES, "only the last chunk is short");
        self.0
            .encrypt(Nonce::from_slice(&chunk_nonce(index)), Payload { msg: plain, aad: &chunk_aad(index, last) })
            .expect("AES-GCM seals any chunk")
    }

    pub fn open_chunk(&self, index: u64, last: bool, sealed: &[u8]) -> Result<Vec<u8>, Refused> {
        self.0
            .decrypt(Nonce::from_slice(&chunk_nonce(index)), Payload { msg: sealed, aad: &chunk_aad(index, last) })
            .map_err(|_| Refused::Chunk { index })
    }
}

/// How many chunks an object of `plain` bytes has: an empty one has one,
/// empty, so even it is sealed.
pub fn chunk_count(plain: u64) -> u64 {
    if plain == 0 {
        1
    } else {
        plain.div_ceil(CHUNK_BYTES)
    }
}

/// The sealed size of an object of `plain` bytes.
pub fn sealed_len(plain: u64) -> u64 {
    HEADER_BYTES + plain + chunk_count(plain) * TAG_BYTES
}

/// The plaintext size of a sealed object of `sealed` bytes, if one has it.
pub fn plain_len(sealed: u64) -> Option<u64> {
    let body = sealed.checked_sub(HEADER_BYTES)?;
    let (full, rest) = (body / SEALED_CHUNK_BYTES, body % SEALED_CHUNK_BYTES);
    let plain = match rest {
        0 => full.checked_mul(CHUNK_BYTES).filter(|plain| *plain > 0)?,
        rest if rest >= TAG_BYTES => full * CHUNK_BYTES + rest - TAG_BYTES,
        _ => return None,
    };
    (sealed_len(plain) == sealed).then_some(plain)
}

/// What a read of plaintext `range` (within an object of `plain` bytes)
/// fetches: its chunks, and the sealed bytes that hold them.
pub fn span(plain: u64, range: &Range<u64>) -> (Range<u64>, Range<u64>) {
    assert!(range.start <= range.end && range.end <= plain, "a range within the object");
    let count = chunk_count(plain);
    let first = (range.start / CHUNK_BYTES).min(count - 1);
    let last = if range.end > range.start { (range.end - 1) / CHUNK_BYTES } else { first };
    let start = HEADER_BYTES + first * SEALED_CHUNK_BYTES;
    let end = (HEADER_BYTES + (last + 1) * SEALED_CHUNK_BYTES).min(sealed_len(plain));
    (first..last + 1, start..end)
}

pub fn header(salt: &[u8; SALT_BYTES]) -> [u8; HEADER_BYTES as usize] {
    let mut header = [0u8; HEADER_BYTES as usize];
    header[..8].copy_from_slice(OBJECT_MAGIC);
    header[8..].copy_from_slice(salt);
    header
}

/// The salt a sealed object's first bytes carry.
pub fn salt_of(bytes: &[u8]) -> Result<[u8; SALT_BYTES], Refused> {
    let header = bytes.get(..HEADER_BYTES as usize).ok_or(Refused::Header)?;
    if &header[..8] != OBJECT_MAGIC {
        return Err(Refused::Header);
    }
    Ok(header[8..].try_into().expect("32 bytes"))
}

/// A whole object, sealed.
pub fn seal_object(key: &ObjectKey, salt: &[u8; SALT_BYTES], plain: &[u8]) -> Vec<u8> {
    let mut sealer = Sealer::new(key, salt);
    let mut sealed = sealer.push(plain);
    sealed.extend(sealer.finish());
    sealed
}

/// Seals an object as its bytes arrive (a multipart upload's parts). A chunk
/// is sealed once a byte past it has arrived, so the last is known as last.
pub struct Sealer<'a> {
    key: &'a ObjectKey,
    header: Option<[u8; HEADER_BYTES as usize]>,
    next: u64,
    pending: Vec<u8>,
}

impl<'a> Sealer<'a> {
    pub fn new(key: &'a ObjectKey, salt: &[u8; SALT_BYTES]) -> Self {
        Self { key, header: Some(header(salt)), next: 0, pending: Vec::new() }
    }

    pub fn push(&mut self, plain: &[u8]) -> Vec<u8> {
        let mut out = self.header.take().map(Vec::from).unwrap_or_default();
        self.pending.extend_from_slice(plain);
        let chunk = CHUNK_BYTES as usize;
        let mut taken = 0;
        while self.pending.len() - taken > chunk {
            out.extend(self.key.seal_chunk(self.next, false, &self.pending[taken..taken + chunk]));
            self.next += 1;
            taken += chunk;
        }
        self.pending.drain(..taken);
        out
    }

    pub fn finish(mut self) -> Vec<u8> {
        let mut out = self.header.take().map(Vec::from).unwrap_or_default();
        out.extend(self.key.seal_chunk(self.next, true, &self.pending));
        out
    }
}

/// Opens the sealed bytes of a read's span, as they arrive: whole chunks
/// are opened, and only the plaintext the read asked for is returned.
pub struct Opener {
    key: ObjectKey,
    plain: u64,
    next: u64,
    end_chunk: u64,
    /// Plaintext offsets the read wants.
    want: Range<u64>,
    buffer: Vec<u8>,
}

impl Opener {
    pub fn new(key: ObjectKey, plain: u64, want: Range<u64>) -> Self {
        let (chunks, _) = span(plain, &want);
        Self { key, plain, next: chunks.start, end_chunk: chunks.end, want, buffer: Vec::new() }
    }

    fn chunk_len(&self, index: u64) -> u64 {
        let start = index * CHUNK_BYTES;
        self.plain.saturating_sub(start).min(CHUNK_BYTES) + TAG_BYTES
    }

    /// The plaintext `sealed` completes, in order.
    pub fn push(&mut self, sealed: &[u8]) -> Result<Vec<u8>, Refused> {
        self.buffer.extend_from_slice(sealed);
        let mut out = Vec::new();
        while self.next < self.end_chunk {
            let len = self.chunk_len(self.next) as usize;
            if self.buffer.len() < len {
                break;
            }
            let last = self.next + 1 == chunk_count(self.plain);
            let plain = self.key.open_chunk(self.next, last, &self.buffer[..len])?;
            self.buffer.drain(..len);
            let at = self.next * CHUNK_BYTES;
            let from = self.want.start.saturating_sub(at).min(plain.len() as u64) as usize;
            let to = (self.want.end.saturating_sub(at)).min(plain.len() as u64) as usize;
            out.extend_from_slice(&plain[from..to.max(from)]);
            self.next += 1;
        }
        if self.next == self.end_chunk && !self.buffer.is_empty() {
            return Err(Refused::Length);
        }
        Ok(out)
    }

    /// Every chunk the span needs arrived and opened.
    pub fn finish(&self) -> Result<(), Refused> {
        if self.next == self.end_chunk && self.buffer.is_empty() {
            Ok(())
        } else {
            Err(Refused::Length)
        }
    }
}

pub struct RecordKey(Aes256Gcm);

impl RecordKey {
    /// `plain`, sealed as a record of `kind`, under a fresh nonce.
    pub fn seal(&self, kind: &'static str, plain: &[u8]) -> Vec<u8> {
        let mut nonce = [0u8; NONCE_BYTES];
        getrandom::fill(&mut nonce).expect("OS random source unavailable");
        let aad = [RECORD_MAGIC.as_slice(), kind.as_bytes()].concat();
        let sealed = self
            .0
            .encrypt(Nonce::from_slice(&nonce), Payload { msg: plain, aad: &aad })
            .expect("AES-GCM seals any record");
        [RECORD_MAGIC.as_slice(), &nonce, &sealed].concat()
    }

    pub fn open(&self, kind: &'static str, sealed: &[u8]) -> Result<Vec<u8>, Refused> {
        let refused = Refused::Record { kind };
        if sealed.len() < RECORD_HEADER_BYTES || &sealed[..8] != RECORD_MAGIC {
            return Err(refused);
        }
        let aad = [RECORD_MAGIC.as_slice(), kind.as_bytes()].concat();
        self.0
            .decrypt(
                Nonce::from_slice(&sealed[8..RECORD_HEADER_BYTES]),
                Payload { msg: &sealed[RECORD_HEADER_BYTES..], aad: &aad },
            )
            .map_err(|_| refused)
    }
}

/// Where the store's history stands: its last commit and the hash of every
/// commit up to it. The counter outside the bucket holds the latest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Head {
    pub seq: u64,
    pub hash: [u8; 32],
}

impl Head {
    pub const GENESIS: Head = Head { seq: 0, hash: [0; 32] };

    /// The head after the commit whose batch is `batch` (its exact bytes).
    pub fn next(&self, batch: &[u8]) -> Head {
        let seq = self.seq.checked_add(1).expect("a sealed bucket's commits fit in u64");
        let hash = Sha256::new()
            .chain_update(CHAIN_LABEL)
            .chain_update(self.hash)
            .chain_update(seq.to_be_bytes())
            .chain_update(Sha256::digest(batch))
            .finalize()
            .into();
        Head { seq, hash }
    }
}

pub fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes)
}

pub fn unb64<const N: usize>(text: &str) -> Option<[u8; N]> {
    base64::engine::general_purpose::STANDARD_NO_PAD.decode(text).ok()?.try_into().ok()
}

pub fn unb64_vec(text: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD_NO_PAD.decode(text).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> MasterKey {
        MasterKey::new([7; KEY_BYTES])
    }

    fn sizes() -> Vec<u64> {
        let c = CHUNK_BYTES;
        vec![0, 1, c - 1, c, c + 1, 2 * c, 3 * c - 7, 3 * c]
    }

    fn bytes(len: u64) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    #[test]
    fn every_size_seals_to_its_length_and_back() {
        for size in sizes() {
            let salt = [size as u8; SALT_BYTES];
            let k = key().object("cells/a/own.json", &salt);
            let sealed = seal_object(&k, &salt, &bytes(size));
            assert_eq!(sealed.len() as u64, sealed_len(size), "size {size}");
            assert_eq!(plain_len(sealed.len() as u64), Some(size), "size {size}");
            assert_eq!(salt_of(&sealed), Ok(salt));
            let mut opener = Opener::new(key().object("cells/a/own.json", &salt), size, 0..size);
            let plain = opener.push(&sealed[HEADER_BYTES as usize..]).unwrap();
            opener.finish().unwrap();
            assert_eq!(plain, bytes(size), "size {size}");
        }
    }

    #[test]
    fn a_streamed_seal_is_the_whole_seal_whatever_the_cuts() {
        let size = 3 * CHUNK_BYTES - 7;
        let plain = bytes(size);
        let salt = [1; SALT_BYTES];
        let k = key().object("r2/b/k", &salt);
        let whole = seal_object(&k, &salt, &plain);
        for cut in [1usize, 1000, CHUNK_BYTES as usize, CHUNK_BYTES as usize + 3] {
            let mut sealer = Sealer::new(&k, &salt);
            let mut streamed = Vec::new();
            for part in plain.chunks(cut) {
                streamed.extend(sealer.push(part));
            }
            streamed.extend(sealer.finish());
            assert_eq!(streamed, whole, "cut {cut}");
        }
    }

    #[test]
    fn a_range_reads_only_its_chunks_and_returns_only_its_bytes() {
        let size = 3 * CHUNK_BYTES - 7;
        let plain = bytes(size);
        let salt = [2; SALT_BYTES];
        let sealed = seal_object(&key().object("x", &salt), &salt, &plain);
        let c = CHUNK_BYTES;
        for want in [0..0, 0..1, 5..c, c - 1..c + 1, c..2 * c, 2 * c + 1..size, size..size] {
            let (_, bytes) = span(size, &want);
            let mut opener = Opener::new(key().object("x", &salt), size, want.clone());
            let got = opener.push(&sealed[bytes.start as usize..bytes.end as usize]).unwrap();
            opener.finish().unwrap();
            assert_eq!(got, plain[want.start as usize..want.end as usize], "{want:?}");
        }
    }

    #[test]
    fn another_name_salt_or_key_does_not_open() {
        let salt = [3; SALT_BYTES];
        let sealed = seal_object(&key().object("a", &salt), &salt, b"hello");
        let body = &sealed[HEADER_BYTES as usize..];
        let open = |k: ObjectKey| Opener::new(k, 5, 0..5).push(body);
        assert!(open(key().object("a", &salt)).is_ok());
        assert_eq!(open(key().object("b", &salt)), Err(Refused::Chunk { index: 0 }));
        assert_eq!(open(key().object("a", &[4; SALT_BYTES])), Err(Refused::Chunk { index: 0 }));
        assert_eq!(open(MasterKey::new([8; KEY_BYTES]).object("a", &salt)), Err(Refused::Chunk { index: 0 }));
    }

    #[test]
    fn a_changed_reordered_or_truncated_object_does_not_open() {
        let size = 2 * CHUNK_BYTES + 10;
        let salt = [5; SALT_BYTES];
        let k = || key().object("x", &salt);
        let sealed = seal_object(&k(), &salt, &bytes(size));
        let body = sealed[HEADER_BYTES as usize..].to_vec();
        let whole = |b: &[u8]| {
            let mut o = Opener::new(k(), size, 0..size);
            o.push(b).and_then(|p| o.finish().map(|()| p))
        };
        let mut flipped = body.clone();
        flipped[100] ^= 1;
        assert_eq!(whole(&flipped), Err(Refused::Chunk { index: 0 }));
        let s = SEALED_CHUNK_BYTES as usize;
        let swapped = [&body[s..2 * s], &body[..s], &body[2 * s..]].concat();
        assert_eq!(whole(&swapped), Err(Refused::Chunk { index: 0 }));
        // cut after a full chunk: the full chunk was sealed as not last
        let short = (2 * CHUNK_BYTES, &body[..2 * s]);
        let mut o = Opener::new(k(), short.0, 0..short.0);
        assert_eq!(o.push(short.1), Err(Refused::Chunk { index: 1 }));
        // fewer bytes than the size says
        assert_eq!(whole(&body[..body.len() - 1]), Err(Refused::Length));
        assert_eq!(whole(&[body.as_slice(), b"x"].concat()), Err(Refused::Length));
    }

    #[test]
    fn plain_len_refuses_sizes_no_object_has() {
        assert_eq!(plain_len(0), None);
        assert_eq!(plain_len(HEADER_BYTES + TAG_BYTES - 1), None);
        assert_eq!(plain_len(HEADER_BYTES + TAG_BYTES), Some(0));
        assert_eq!(plain_len(HEADER_BYTES + SEALED_CHUNK_BYTES + TAG_BYTES), None, "an empty chunk after a full one");
        assert_eq!(plain_len(HEADER_BYTES + SEALED_CHUNK_BYTES), Some(CHUNK_BYTES));
    }

    #[test]
    fn a_record_opens_only_as_its_kind_under_its_key() {
        let records = key().records();
        let sealed = records.seal("journal", b"{}");
        assert_eq!(records.open("journal", &sealed).unwrap(), b"{}");
        assert_eq!(records.open("counter", &sealed), Err(Refused::Record { kind: "counter" }));
        assert_eq!(MasterKey::new([9; KEY_BYTES]).records().open("journal", &sealed), Err(Refused::Record { kind: "journal" }));
        assert_ne!(records.seal("journal", b"{}"), sealed, "a fresh nonce each time");
    }

    #[test]
    fn the_chain_depends_on_every_batch_and_its_order() {
        let a = Head::GENESIS.next(b"a").next(b"b");
        let b = Head::GENESIS.next(b"b").next(b"a");
        assert_eq!(a.seq, 2);
        assert_ne!(a, b);
        assert_eq!(a, Head::GENESIS.next(b"a").next(b"b"));
    }
}
