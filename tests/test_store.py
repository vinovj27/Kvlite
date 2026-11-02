import tempfile
import threading
import unittest
from pathlib import Path

from kvlite.store import Durability, KVStore, encode


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000_000.0

    def __call__(self) -> float:
        return self.now


class StoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.clock = FakeClock()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def open(self) -> KVStore:
        return KVStore(self.dir, Durability.BUFFERED, self.clock)

    def test_set_get_delete(self):
        with self.open() as s:
            s.set("user:1", "Ada")
            self.assertEqual(s.get("user:1"), "Ada")
            self.assertTrue(s.delete("user:1"))
            self.assertIsNone(s.get("user:1"))
            self.assertFalse(s.delete("user:1"))

    def test_data_survives_restart(self):
        with self.open() as s:
            s.set("a", "1")
            s.set("b", "2")
            s.set("a", "3")
            s.delete("b")
        with self.open() as s:
            self.assertEqual(s.get("a"), "3")
            self.assertIsNone(s.get("b"))
            self.assertEqual(s.stats().recovered, 4)

    def test_torn_write_is_truncated_and_store_keeps_working(self):
        with self.open() as s:
            s.set("safe", "yes")
        with open(self.dir / "wal.log", "ab") as fh:
            fh.write(b"SET\tdG9ybg==\tcGFy")
        with self.open() as s:
            self.assertEqual(s.get("safe"), "yes")
            self.assertGreater(s.stats().corrupt_tail_bytes, 0)
            s.set("after", "crash")
        with self.open() as s:
            self.assertEqual(s.get("after"), "crash")
            self.assertEqual(s.stats().corrupt_tail_bytes, 0)

    def test_bad_checksum_is_never_applied(self):
        with self.open() as s:
            s.set("k", "good")
        forged = encode("SET", "k", "evil").rsplit(b"\t", 1)[0] + b"\tdeadbeef\n"
        with open(self.dir / "wal.log", "ab") as fh:
            fh.write(forged)
        with self.open() as s:
            self.assertEqual(s.get("k"), "good")

    def test_ttl_expiry_in_memory_and_across_restart(self):
        with self.open() as s:
            s.set("session", "abc", ttl=5)
            s.set("forever", "x")
            self.assertAlmostEqual(s.ttl("session"), 5)
            self.assertEqual(s.ttl("forever"), -1)
            self.clock.now += 5
            self.assertIsNone(s.get("session"))
            s.set("short", "y", ttl=1)
        self.clock.now += 2
        with self.open() as s:
            self.assertIsNone(s.get("short"))
            self.assertEqual(s.keys(), ["forever"])

    def test_incr_is_atomic_under_threads(self):
        with self.open() as s:
            def bump():
                for _ in range(500):
                    s.incr("hits")

            threads = [threading.Thread(target=bump) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertEqual(s.get("hits"), "4000")
            s.set("name", "Ada")
            with self.assertRaises(ValueError):
                s.incr("name")

    def test_compaction_shrinks_log_and_preserves_data(self):
        with self.open() as s:
            for i in range(1_000):
                s.set("counter", str(i))
            s.set("tmp", "gone")
            s.delete("tmp")
            self.assertGreater(s.stats().wal_bytes, 0)
            s.compact()
            self.assertEqual(s.stats().wal_bytes, 0)
            s.set("post", "compact")
        with self.open() as s:
            self.assertEqual(s.get("counter"), "999")
            self.assertEqual(s.get("post"), "compact")
            self.assertIsNone(s.get("tmp"))
            self.assertEqual(s.stats().recovered, 2)

    def test_any_text_round_trips(self):
        key, value = "naïve\tkey", "line1\nline2\t— ünïcödé ✓"
        with self.open() as s:
            s.set(key, value)
        with self.open() as s:
            self.assertEqual(s.get(key), value)
        self.assertNotIn(b"line1", (self.dir / "wal.log").read_bytes())

    def test_concurrent_writes_with_compaction_are_all_durable(self):
        with self.open() as s:
            def write(t):
                for i in range(1_000):
                    s.set(f"t{t}:{i}", str(i))
                    if t == 0 and i % 250 == 0:
                        s.compact()

            threads = [threading.Thread(target=write, args=(t,)) for t in range(6)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        with self.open() as s:
            self.assertEqual(len(s.keys("t")), 6_000)
            self.assertEqual(s.get("t5:999"), "999")

    def test_fsync_mode_is_durable(self):
        with KVStore(self.dir, Durability.FSYNC) as s:
            s.set("k", "v")
        with KVStore(self.dir) as s:
            self.assertEqual(s.get("k"), "v")

    def test_validation(self):
        with self.open() as s:
            with self.assertRaises(ValueError):
                s.set("", "x")
            with self.assertRaises(TypeError):
                s.set("k", 5)
            with self.assertRaises(ValueError):
                s.set("k", "v", ttl=-1)


if __name__ == "__main__":
    unittest.main()
