import asyncio
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from kvlite import KVClient, KVServer, KVStore
from kvlite.store import Durability


class ServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = KVStore(self._tmp.name, Durability.BUFFERED)
        self.server = KVServer(self.store, port=0)
        self.loop = asyncio.new_event_loop()
        ready = threading.Event()

        def run():
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self.server.start())
            ready.set()
            self.loop.run_forever()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        ready.wait(5)

    def tearDown(self) -> None:
        asyncio.run_coroutine_threadsafe(self.server.stop(), self.loop).result(5)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()
        self.store.close()
        self._tmp.cleanup()

    def test_protocol_round_trip(self):
        with KVClient(port=self.server.port) as c:
            self.assertEqual(c.send("PING"), "+PONG")
            self.assertEqual(c.send("SET greeting hello world"), "+OK")
            self.assertEqual(c.send("GET greeting"), "$hello world")
            self.assertEqual(c.send("SETEX token 60 abc123"), "+OK")
            self.assertIn(c.send("TTL token"), (":59", ":60"))
            self.assertEqual(c.send("TTL missing"), ":-2")
            self.assertEqual(c.send("INCR visits"), ":1")
            self.assertEqual(c.send("INCR visits"), ":2")
            self.assertEqual(c.send("KEYS"), "*3 greeting token visits")
            self.assertEqual(c.send("DEL greeting"), ":1")
            self.assertEqual(c.send("GET greeting"), "(nil)")
            self.assertEqual(c.send("COMPACT"), "+OK")
            self.assertTrue(c.send("STATS").startswith("+keys=2"))
            self.assertTrue(c.send("FLY").startswith("-ERR unknown command"))
            self.assertTrue(c.send("SETEX k nope v").startswith("-ERR"))
            self.assertTrue(c.send("GET").startswith("-ERR"))
            self.assertEqual(c.send("QUIT"), "+BYE")

    def test_many_clients_in_parallel(self):
        def session(n):
            ok = 0
            with KVClient(port=self.server.port) as c:
                for i in range(100):
                    c.send(f"SET c{n}:{i} {i}")
                    ok += c.send(f"GET c{n}:{i}") == f"${i}"
                    c.send("INCR shared")
            return ok

        with ThreadPoolExecutor(10) as pool:
            self.assertEqual(sum(pool.map(session, range(10))), 1_000)
        self.assertEqual(len(self.store.keys("c")), 1_000)
        self.assertEqual(self.store.get("shared"), "1000")


if __name__ == "__main__":
    unittest.main()
