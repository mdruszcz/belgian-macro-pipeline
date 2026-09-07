"""Run the local page builder service (`make builder`).

Binds 127.0.0.1 only, mints a session token for this process, prints the one
URL that works, and serves until Ctrl-C. Nothing is written to disk until you
save, and nothing becomes public until you publish (claude.md rule 32).
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.builder.service import (  # noqa: E402
    DEFAULT_PORT,
    BuilderConfig,
    HostRefused,
    make_server,
)
from src.pages.metadata import MetadataError  # noqa: E402
from src.pages.registry import RegistryError  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Serve the local BelPulse page builder API on loopback.",
        epilog=(
            "Reads public/data/national.json and the published metadata at startup: "
            "run `make exports` first if you have run `make clean`."
        ),
    )
    ap.add_argument(
        "--host",
        default="127.0.0.1",
        help="127.0.0.1 or localhost. Any other value is refused: this service writes files.",
    )
    ap.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"default {DEFAULT_PORT}")
    return ap


def main(argv=None) -> int:
    args = _parser().parse_args(argv)

    try:
        config = BuilderConfig(host=args.host, port=args.port)
    except HostRefused as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (MetadataError, RegistryError) as exc:
        # Fatal, naming the missing path, rather than a repeating opaque 500
        # once a request arrives.
        print(f"error: {exc}", file=sys.stderr)
        print("hint: run `make exports` if you have run `make clean`.", file=sys.stderr)
        return 2

    try:
        server = make_server(config)
    except OSError as exc:
        # No fallback port, ever: a fallback prints a working URL while a
        # stale tab still holds a token for a dead process.
        print(f"error: cannot serve on port {config.port}: {exc.strerror}", file=sys.stderr)
        return 2

    print("BelPulse builder service (Batch 12: page/block editing shell).")
    print("Open exactly this URL -- the token is this process's and is not stored anywhere:")
    print(f"  {config.token_url}")
    print("Ctrl-C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
