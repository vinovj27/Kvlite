from __future__ import annotations

import asyncio
import signal

from .store import KVStore

HELP = "PING SET SETEX GET DEL INCR TTL KEYS COMPACT STATS QUIT"


class KVServer:
    def __init__(self, store: KVStore, host: str = "127.0.0.1", port: int = 6380) -> None:
        self.store = store
        self.host = host
        self.port = port
        self.clients = 0
        self.commands = 0
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.clients += 1
        try:
            while line := await reader.readline():
                text = line.decode(errors="replace").strip()
                if not text:
                    continue
                self.commands += 1
                reply = await asyncio.to_thread(self.execute, text)
                writer.write((reply + "\n").encode())
                await writer.drain()
                if reply == "+BYE":
                    break
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self.clients -= 1
            writer.close()

    def execute(self, line: str) -> str:
        parts = line.split(" ", 2)
        cmd = parts[0].upper()
        args = parts[1:]
        try:
            if cmd == "PING":
                return "+PONG"
            if cmd == "SET":
                key, value = self._need(args, 2)
                self.store.set(key, value)
                return "+OK"
            if cmd == "SETEX":
                key, rest = self._need(args, 2)
                seconds, _, value = rest.partition(" ")
                if not value:
                    raise ValueError("usage: SETEX key seconds value")
                self.store.set(key, value, ttl=float(seconds))
                return "+OK"
            if cmd == "GET":
                (key,) = self._need(args, 1)
                value = self.store.get(key)
                return "(nil)" if value is None else f"${value}"
            if cmd == "DEL":
                (key,) = self._need(args, 1)
                return f":{int(self.store.delete(key))}"
            if cmd == "INCR":
                (key,) = self._need(args, 1)
                return f":{self.store.incr(key)}"
            if cmd == "TTL":
                (key,) = self._need(args, 1)
                ttl = self.store.ttl(key)
                return ":-2" if ttl is None else f":{int(ttl)}"
            if cmd == "KEYS":
                keys = self.store.keys(args[0] if args else "")
                return f"*{len(keys)}" + ("" if not keys else " " + " ".join(keys))
            if cmd == "COMPACT":
                self.store.compact()
                return "+OK"
            if cmd == "STATS":
                s = self.store.stats()
                return (f"+keys={s.keys} wal_records={s.wal_records} wal_bytes={s.wal_bytes} "
                        f"recovered={s.recovered} compactions={s.compactions} clients={self.clients} commands={self.commands}")
            if cmd == "QUIT":
                return "+BYE"
            return f"-ERR unknown command '{cmd}'. Try: {HELP}"
        except ValueError as e:
            return f"-ERR {e}"

    @staticmethod
    def _need(args: list[str], n: int) -> list[str]:
        if n == 1 and args:
            return [args[0].split(" ", 1)[0]]
        if len(args) < n:
            raise ValueError("wrong number of arguments")
        return args[:n]


async def serve(data_dir: str, host: str, port: int, durability: str) -> None:
    store = KVStore(data_dir, durability)
    server = KVServer(store, host, port)
    await server.start()
    s = store.stats()
    print(f"KVLite listening on {host}:{server.port} · data in {data_dir} · recovered {s.recovered} records")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    try:
        await stop.wait()
    finally:
        await server.stop()
        store.close()
        print("KVLite stopped cleanly")
