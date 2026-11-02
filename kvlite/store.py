from __future__ import annotations

import base64
import os
import threading
import time
import zlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Iterator

WAL = "wal.log"
SNAPSHOT = "snapshot.db"


class Durability(str, Enum):
    FSYNC = "fsync"
    BUFFERED = "buffered"


@dataclass(frozen=True)
class Stats:
    keys: int
    wal_records: int
    wal_bytes: int
    recovered: int
    corrupt_tail_bytes: int
    compactions: int


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _unb64(s: str) -> str:
    return base64.b64decode(s.encode(), validate=True).decode()


def encode(op: str, key: str, value: str = "", expires_at: float = 0.0) -> bytes:
    body = f"{op}\t{_b64(key)}\t{_b64(value)}\t{expires_at!r}"
    return f"{body}\t{zlib.crc32(body.encode()):08x}\n".encode()


class KVStore:
    """Durable key-value store: write-ahead log + in-memory hash map + snapshot compaction."""

    def __init__(self, path: str | os.PathLike, durability: Durability = Durability.FSYNC,
                 clock: Callable[[], float] = time.time) -> None:
        self.dir = Path(path)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.durability = Durability(durability)
        self.clock = clock
        self._data: dict[str, tuple[str, float]] = {}
        self._lock = threading.RLock()
        self._recovered = 0
        self._corrupt_tail = 0
        self._compactions = 0
        self._load(self.dir / SNAPSHOT, truncate=False)
        self._wal_records = self._load(self.dir / WAL, truncate=True)
        self._wal = open(self.dir / WAL, "ab")

    def __enter__(self) -> "KVStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def set(self, key: str, value: str, ttl: float | None = None) -> None:
        self._check_key(key)
        if not isinstance(value, str):
            raise TypeError("value must be a string")
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl must be positive")
        expires_at = self.clock() + ttl if ttl else 0.0
        with self._lock:
            self._append(encode("SET", key, value, expires_at))
            self._data[key] = (value, expires_at)

    def get(self, key: str) -> str | None:
        with self._lock:
            entry = self._live(key)
            return entry[0] if entry else None

    def delete(self, key: str) -> bool:
        self._check_key(key)
        with self._lock:
            existed = self._live(key) is not None
            self._append(encode("DEL", key))
            self._data.pop(key, None)
            return existed

    def incr(self, key: str, by: int = 1) -> int:
        with self._lock:
            entry = self._live(key)
            try:
                current = int(entry[0]) if entry else 0
            except ValueError:
                raise ValueError("value is not an integer") from None
            new = current + by
            self.set(key, str(new), ttl=(entry[1] - self.clock()) if entry and entry[1] else None)
            return new

    def ttl(self, key: str) -> float | None:
        with self._lock:
            entry = self._live(key)
            if entry is None:
                return None
            return -1.0 if entry[1] == 0 else max(0.0, entry[1] - self.clock())

    def keys(self, prefix: str = "") -> list[str]:
        with self._lock:
            now = self.clock()
            return sorted(k for k, (_, exp) in self._data.items() if k.startswith(prefix) and not (exp and exp <= now))

    def compact(self) -> None:
        with self._lock:
            now = self.clock()
            tmp = self.dir / (SNAPSHOT + ".tmp")
            with open(tmp, "wb") as fh:
                for key, (value, exp) in self._data.items():
                    if not (exp and exp <= now):
                        fh.write(encode("SET", key, value, exp))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.dir / SNAPSHOT)
            self._fsync_dir()
            self._wal.close()
            self._wal = open(self.dir / WAL, "wb")
            self._wal_records = 0
            self._compactions += 1

    def stats(self) -> Stats:
        with self._lock:
            self._wal.flush()
            return Stats(len(self.keys()), self._wal_records, (self.dir / WAL).stat().st_size,
                         self._recovered, self._corrupt_tail, self._compactions)

    def close(self) -> None:
        with self._lock:
            if not self._wal.closed:
                self._wal.flush()
                os.fsync(self._wal.fileno())
                self._wal.close()

    def _append(self, record: bytes) -> None:
        self._wal.write(record)
        self._wal.flush()
        if self.durability is Durability.FSYNC:
            os.fsync(self._wal.fileno())
        self._wal_records += 1

    def _live(self, key: str) -> tuple[str, float] | None:
        entry = self._data.get(key)
        if entry and entry[1] and entry[1] <= self.clock():
            del self._data[key]
            return None
        return entry

    def _load(self, file: Path, truncate: bool) -> int:
        if not file.exists():
            return 0
        raw = file.read_bytes()
        good = 0
        applied = 0
        for line in self._lines(raw):
            if not self._replay(line):
                break
            good += len(line) + 1
            applied += 1
        self._recovered += applied
        if good < len(raw):
            self._corrupt_tail += len(raw) - good
            if truncate:
                with open(file, "r+b") as fh:
                    fh.truncate(good)
                    os.fsync(fh.fileno())
        return applied

    @staticmethod
    def _lines(raw: bytes) -> Iterator[bytes]:
        start = 0
        while (end := raw.find(b"\n", start)) != -1:
            yield raw[start:end]
            start = end + 1

    def _replay(self, line: bytes) -> bool:
        try:
            text = line.decode()
            body, crc = text.rsplit("\t", 1)
            if f"{zlib.crc32(body.encode()):08x}" != crc:
                return False
            op, key, value, expires_at = body.split("\t")
            if op == "SET":
                self._data[_unb64(key)] = (_unb64(value), float(expires_at))
            elif op == "DEL":
                self._data.pop(_unb64(key), None)
            else:
                return False
            return True
        except (ValueError, UnicodeDecodeError):
            return False

    def _fsync_dir(self) -> None:
        if hasattr(os, "O_DIRECTORY"):
            fd = os.open(self.dir, os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    @staticmethod
    def _check_key(key: str) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        if len(key) > 512:
            raise ValueError("key too long")
