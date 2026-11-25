# lanes.py DIR SECONDS MODE WRITERS: emulate `celld dev`'s store.
# Every op opens a fresh connection (WAL, synchronous=FULL, busy 30 s) as
# LocalStore does. WRITERS threads put LTX-sized objects (BEGIN IMMEDIATE,
# etag sequence, upsert) back to back with a short sleep; one in 50 is a
# 4 MiB blob. A lease thread CASes nodes/dev.json every 3.33 s, timing the
# lock (BEGIN IMMEDIATE) and the rest (statements + commit) apart.
# MODE "shared": the lease lives in objects.sqlite3 with the writers.
# MODE "split": the lease lives in its own leases.sqlite3 beside it.
import os, random, sqlite3, sys, threading, time
d, secs, mode, writers = sys.argv[1], float(sys.argv[2]), sys.argv[3], int(sys.argv[4])
BIG = float(sys.argv[5]) if len(sys.argv) > 5 else 0.02
BIGSIZE = int(float(sys.argv[6]) * (1 << 20)) if len(sys.argv) > 6 else 4 << 20
stop = time.time() + secs
SCHEMA = """CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, body BLOB NOT NULL,
 etag INTEGER NOT NULL, modified_ms INTEGER NOT NULL, attributes TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS store_sequence (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), next_etag INTEGER NOT NULL);
 INSERT OR IGNORE INTO store_sequence(singleton, next_etag) VALUES (1, 1);"""
def path(name): return os.path.join(d, name)
def init(name):
    c = sqlite3.connect(path(name), isolation_level=None); c.execute("pragma journal_mode=WAL"); c.executescript(SCHEMA); c.close()
def connect(name):
    c = sqlite3.connect(path(name), timeout=30, isolation_level=None); c.execute("pragma synchronous=FULL"); return c
def put(c, key, body):
    c.execute("SELECT etag FROM objects WHERE key=?", (key,)).fetchone()
    etag = c.execute("UPDATE store_sequence SET next_etag=next_etag+1 WHERE singleton=1 RETURNING next_etag-1").fetchone()[0]
    c.execute("INSERT INTO objects(key,body,etag,modified_ms,attributes) VALUES(?,?,?,?,'') ON CONFLICT(key) DO UPDATE SET body=excluded.body, etag=excluded.etag", (key, body, etag, int(time.time()*1000)))
    c.execute("COMMIT")
init("objects.sqlite3")
lease_db = "objects.sqlite3" if mode == "shared" else "leases.sqlite3"
init(lease_db)
counts = [0]*writers
def writer(i):
    n = 0
    while time.time() < stop:
        big = random.random() < BIG
        body = os.urandom(BIGSIZE if big else random.choice((2048, 4096, 16384)))
        c = connect("objects.sqlite3"); c.execute("BEGIN IMMEDIATE"); put(c, f"cells/w{i}/{n % 500}", body); c.close()
        n += 1; counts[i] = n
        time.sleep(0.005)
rows = []
def lease():
    while time.time() < stop:
        t0 = time.time()
        c = connect(lease_db); c.execute("BEGIN IMMEDIATE"); t1 = time.time()
        put(c, "nodes/dev.json", os.urandom(800)); c.close(); t2 = time.time()
        rows.append((t1 - t0, t2 - t1, t2 - t0))
        time.sleep(max(0, 3.33 - (t2 - t0)))
ts = [threading.Thread(target=writer, args=(i,)) for i in range(writers)] + [threading.Thread(target=lease)]
[t.start() for t in ts]; [t.join() for t in ts]
lock = sorted(r[0] for r in rows); rest = sorted(r[1] for r in rows); tot = sorted(r[2] for r in rows)
q = lambda v, p: v[min(len(v)-1, int(p*len(v)))]*1000
print(f"mode={mode} writers={writers} writes={sum(counts)} ({sum(counts)/secs:.0f}/s) renewals={len(rows)}")
print(f"  lock wait ms: p50 {q(lock,.5):.0f} p90 {q(lock,.9):.0f} max {lock[-1]*1000:.0f}")
print(f"  write+commit ms: p50 {q(rest,.5):.0f} p90 {q(rest,.9):.0f} max {rest[-1]*1000:.0f}")
print(f"  renewal ms: p50 {q(tot,.5):.0f} p90 {q(tot,.9):.0f} max {tot[-1]*1000:.0f}; over 2500 ms: {sum(t > 2.5 for t in tot)}")
