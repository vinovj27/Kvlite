# KVLite

A crash-safe key-value database written from scratch in Python — write-ahead logging, checksummed records, crash recovery, snapshot compaction, TTL expiry and an asyncio TCP server with a Redis-style protocol. Standard library only.

It is a small, readable implementation of the ideas behind Redis's append-only file, RocksDB's write-ahead log and Postgres crash recovery.

![Python](https://img.shields.io/badge/Python-3.10+-blue) ![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen) ![Tests](https://img.shields.io/badge/tests-13%20passing-brightgreen)

## How it works

```
 client ──TCP──▶ asyncio server ──▶ KVStore
                                     │
                    1. append record to wal.log   (CRC32 + base64, optional fsync)
                    2. apply to in-memory dict
                                     │
                    COMPACT ─▶ snapshot.tmp ─fsync─▶ atomic rename ─▶ truncate WAL
```

- **Write-ahead log.** Every change is appended to `wal.log` before it is applied in memory, so an acknowledged write is never lost.
- **Checksummed records.** Each line is `OP  key  value  expires_at  crc32`. Keys and values are base64-encoded, so tabs, newlines and Unicode are all safe.
- **Crash recovery.** On startup the store replays `snapshot.db` then `wal.log`. A torn or corrupted tail (power cut mid-write) fails its checksum; replay stops there and the log is truncated to the last good record.
- **Compaction.** Live keys are written to a temp file, fsynced, and atomically renamed over the old snapshot, so the snapshot on disk is always complete. Deleted and expired keys disappear.
- **Concurrency.** An asyncio server handles many clients; storage calls run in worker threads behind one lock, so `INCR` is atomic.
- **Durability modes.** `fsync` (survives power loss) or `buffered` (survives a process crash, much faster).

## Run it

Python 3.10 or newer, no installs needed.

```bash
python -m kvlite serve                 # start on port 6380, data in ./data
python -m kvlite cli                   # interactive client in a second terminal
python -m kvlite bench                 # throughput in both durability modes
python -m unittest discover -s tests   # run the tests
```

```
kvlite> SET user:1 Ada Lovelace
+OK
kvlite> SETEX session:42 60 tok_abc
+OK
kvlite> INCR page_views
:1
kvlite> GET user:1
$Ada Lovelace
kvlite> STATS
+keys=3 wal_records=3 wal_bytes=118 recovered=0 compactions=0 clients=1 commands=5
```

Stop the server with `Ctrl+C` and start it again: your keys are still there.

Or install it as a command with `pip install .`, then run `kvlite serve`.

## Commands

| Command | Reply |
|---|---|
| `SET key value` | `+OK` |
| `SETEX key seconds value` | `+OK` |
| `GET key` | `$value` or `(nil)` |
| `DEL key` | `:1` if deleted, `:0` if missing |
| `INCR key` | `:new_value` |
| `TTL key` | seconds left, `:-1` no expiry, `:-2` missing |
| `KEYS [prefix]` | `*count key1 key2 …` |
| `COMPACT` | `+OK` |
| `STATS` | `+keys=… wal_records=… …` |
| `PING` / `QUIT` | `+PONG` / `+BYE` |

## What the tests prove

- Data survives restart, including deletes and overwrites
- A torn write after a simulated crash is detected and truncated, and new writes then succeed
- A record with a forged checksum is never applied
- TTL expiry works in memory and across restarts
- Compaction shrinks the log to zero and keeps every live key
- 6,000 writes from 6 threads, interleaved with compactions, are all present after restart
- 8 threads × 500 `INCR`s land on exactly 4,000
- 10 TCP clients in parallel read back their own writes

## Benchmark

`python -m kvlite bench` on a small cloud VM, 4 writer threads:

| Mode | Writes/s | Reads/s |
|---|---|---|
| `fsync` | ~7,000 | ~1.1M |
| `buffered` | ~59,000 | ~1.6M |

The gap is the cost of making the disk confirm every write, the trade-off every database makes. Numbers vary by machine.

## Next steps

- Group commit: batch several writers into one fsync
- Sorted on-disk segments (an LSM tree) so data can exceed memory
- Leader/follower replication by streaming the WAL
