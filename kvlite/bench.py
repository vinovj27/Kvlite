from __future__ import annotations

import tempfile
import threading
import time

from .store import Durability, KVStore


def run(ops: int = 100_000, threads: int = 4) -> list[dict]:
    results = []
    for mode in (Durability.FSYNC, Durability.BUFFERED):
        n = min(ops, 2_000) if mode is Durability.FSYNC else ops
        with tempfile.TemporaryDirectory() as tmp, KVStore(tmp, mode) as store:
            def writer(offset: int) -> None:
                for i in range(offset, n, threads):
                    store.set(f"key:{i}", f"value-{i}")

            start = time.perf_counter()
            workers = [threading.Thread(target=writer, args=(t,)) for t in range(threads)]
            for w in workers:
                w.start()
            for w in workers:
                w.join()
            write_s = time.perf_counter() - start

            start = time.perf_counter()
            for i in range(n):
                store.get(f"key:{i}")
            read_s = time.perf_counter() - start
            results.append({"mode": mode.value, "ops": n, "writes_per_s": n / write_s, "reads_per_s": n / read_s})
    return results


def main(ops: int, threads: int) -> None:
    print(f"{'mode':<10} {'ops':>9} {'writes/s':>12} {'reads/s':>12}")
    for r in run(ops, threads):
        print(f"{r['mode']:<10} {r['ops']:>9,} {r['writes_per_s']:>12,.0f} {r['reads_per_s']:>12,.0f}")
