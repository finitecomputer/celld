# The sealed fleet bucket

A fork feature. With `CELLD_BUCKET_SEAL` set, every object celld keeps in
its fleet bucket is sealed before it leaves the node. Whoever holds the
bucket (its provider, its project's administrators, anyone with its
credentials) reads only ciphertext. The node also refuses a bucket that
serves it an older object, hides one of its writes, or lets a second
writer win.

It is meant for a node whose memory its operator cannot read, such as a
confidential sandbox whose key is released only to its measured image. On
an ordinary host, whoever reads the node's memory reads the key.

## What a holder of the bucket sees

| | Plain bucket | Sealed bucket |
|---|---|---|
| An object's bytes (SQLite pages, logs, R2 bodies) | readable | AES-256-GCM, a key per object version |
| An object's attributes (content type, R2 custom metadata) | readable | in the sealed index, not on the object |
| The deployment, ownership records, the peer secret | readable | sealed like everything else |
| Object names, sizes and write times | visible | **still visible** |
| An object's write or delete cut short (a stop past its bound, a kill) | n/a | adopted at its next read, logged |
| An older object of the home's own put back | believed | believed, logged (anti-rollback: a team question, FIN-622) |
| A commit hidden, or the bucket rolled back whole | believed | refused |
| A second node writing the same bucket | can win | fenced; it writes nothing more |

Names stay plain: a listing and a range read need them. They say which
cells, R2 keys and deployments exist, and when each changed. The fragment
cell's R2 keys are a fragment's npub and a blob's SHA-256, so a holder who
already has a file can tell whether a fragment stores it.

## How it works

- **Objects.** An object is a header (a magic number and the version's
  random salt) and its plaintext in 64 KiB chunks. Each chunk is sealed
  with AES-256-GCM under a key derived (HKDF-SHA256) from the bucket's key,
  the object's name and the salt. A chunk's nonce is its index, and its
  additional data says whether it is the last. An object opens only under
  its own name, only in the version the index names, and only whole: a
  changed, reordered, dropped or truncated chunk does not open. A range
  read fetches only the chunks it needs.
- **The index.** The store's index holds each object's latest version: its
  salt, size, ETag and attributes. A list or a head is answered from it;
  the bucket is asked only for bytes. A read asks the bucket for the
  index's ETag and checks every chunk.
- **The chain.** Every commit to the index is a batch of changes. Its hash
  chains to the one before it: `H(label ‖ previous hash ‖ seq ‖ H(batch))`.
  Batches are kept in the bucket as sealed journal records, with a sealed
  snapshot of the whole index every 256 commits.
- **The counter.** The chain's head (its sequence number, its hash and its
  last batch) is kept in a counter object outside the fleet bucket, in
  another bucket, behind other credentials. A commit is a conditional write
  of the counter (`If-Match` its last ETag). A write is acknowledged only
  once its commit has reached the counter. Concurrent writes share one
  commit.
- **A start** reads the counter, opens the newest snapshot at or before
  it, replays the journal after it, and refuses unless the chain reaches
  the counter's exact hash. A bucket rolled back, a journal record hidden,
  or a counter removed while the bucket holds sealed history all refuse
  the start.
- **Every client of the bucket.** The node's bucket and its cells'
  replication (their SQLite pages, `cells/<cell>/ltx/…`) share one sealed
  store per bucket and prefix in the process; a node with
  `CELLD_BUCKET_SEAL` set whose replication finds no sealed store refuses
  to start rather than write a page in the clear. Another program writing
  the same bucket (not celld) is not sealed by this.
- **Two writers.** Every object write is conditional on the version the
  index holds, and every commit on the counter's last ETag. A second
  process loses one of the two, and then refuses every further call,
  reads included, since its index is stale.

## A home's own interrupted work

A home must never be left unable to start by its own crash, its own stop or
its own write cut short. An object's write is its PUT to the bucket, then
its commit (the index's entry, the counter); a delete is the reverse order.
A stop or a kill between the two leaves the bucket holding a version the
index never named, or none. The next read of that name finds the bucket's
version and checks it: a sealed object whose header's salt and whose first
and last chunks open under the home's key for that name is the home's own,
and it is adopted, committed and logged (`sealed_bucket_adopted_version`);
a missing one is the delete, likewise (`sealed_bucket_adopted_delete`). Its
attributes went with its commit, so the last ones the index held stand. A
write that finds such a version in its way adopts it, then writes. A version
that does not open under the key is refused, as before: only the key's
holder writes a version that opens. `celld seal-bucket check` reads every
object, so it adopts them too, and counts them.

A commit's leader runs on a task of its own, and a commit's journal record
stays due until the bucket has it, so no caller that goes away leaves a
hole in the journal.

## Configuration

`CELLD_BUCKET_SEAL` names a JSON file, readable by its owner alone:

```json
{
  "key": "<32 bytes, base64 without padding>",
  "counter": {
    "bucket": "s3://COUNTER-BUCKET",
    "name": "homes/<this fleet's name>",
    "region": "<region>",
    "access_key_id": "<the counter's own credentials>",
    "secret_access_key": "…"
  }
}
```

The fleet bucket's own credentials come from the environment, as without
sealing. Give the counter's credentials a policy that names only the
counter's object, and keep the fleet bucket's away from it. `celld dev`
never seals.

## Moving a bucket into its sealed form

Stop the node, then:

```sh
celld seal-bucket migrate --from s3://FLEET --to s3://FLEET/sealed --region R
celld seal-bucket check --bucket s3://FLEET/sealed --region R
```

`migrate` copies every object, with its attributes, into the sealed form,
then reads every object back from both and compares their SHA-256 before
it writes its mark (`seal-migrated.json`). It never deletes the plain
objects; run again, it skips what it copied, and once marked it does
nothing. Start the node on `s3://FLEET/sealed`. Delete the plain objects
only once the node has served from the sealed form.

## What it costs

- Each write waits for one more conditional write (the counter) after its
  object's write. Concurrent writes share it.
- The previous commit's journal record is written before the next commit:
  about one more PUT per commit.
- The index is in memory: about 200 bytes an object, up to 4,000,000
  objects.

## What it does not protect

- **An older version of an object put back.** It opens under the key, as
  the home's own write cut short does, so it is believed (and logged). The
  counter still refuses a commit hidden or the whole bucket rolled back.
- **A holder of both buckets.** The provider, or the administrators of a
  project that holds both the fleet bucket and the counter, can roll both
  back to an earlier moment together, and a start then believes the
  earlier state. The counter keeps the fleet bucket's own credentials, and
  anyone who has only those, out. Against the provider itself, the counter
  must live where they cannot write it.
- **Names, sizes and timing** (above).
- **A multipart upload's overwrite** is not conditional (S3 completes
  uploads unconditionally here). A second writer can replace a large
  object, and the next read refuses it rather than serving it.
- **Availability.** Whoever holds the bucket can delete objects. The node
  then refuses to serve; it does not serve something else.
