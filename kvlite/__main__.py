import argparse
import asyncio
import os

from . import bench, client, server


def main() -> None:
    parser = argparse.ArgumentParser(prog="kvlite", description="Crash-safe key-value database")
    sub = parser.add_subparsers(dest="command", required=True)

    s = sub.add_parser("serve", help="start the database server")
    s.add_argument("--data", default=os.environ.get("KVLITE_DATA", "data"))
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=int(os.environ.get("KVLITE_PORT", 6380)))
    s.add_argument("--durability", choices=["fsync", "buffered"], default="fsync")

    c = sub.add_parser("cli", help="interactive client")
    c.add_argument("--host", default="127.0.0.1")
    c.add_argument("--port", type=int, default=6380)

    b = sub.add_parser("bench", help="measure write and read throughput")
    b.add_argument("-n", "--ops", type=int, default=100_000)
    b.add_argument("-t", "--threads", type=int, default=4)

    args = parser.parse_args()
    if args.command == "serve":
        try:
            asyncio.run(server.serve(args.data, args.host, args.port, args.durability))
        except KeyboardInterrupt:
            pass
    elif args.command == "cli":
        client.repl(args.host, args.port)
    else:
        bench.main(args.ops, args.threads)


if __name__ == "__main__":
    main()
