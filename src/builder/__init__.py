"""The local page builder's service layer (Batch 11).

Local only, by construction: it binds `127.0.0.1`, mints a per-process session
token, and exists so a person editing a page can validate, save, publish and
restore page documents with the SAME validator and the SAME renderer the
public build uses. Nothing it produces requires a server to view (claude.md
rule 30), and it never commits or pushes (rule 34).

    BuilderConfig(host=..., port=...)     one server's settings; the metadata
                                          bundle and the block registry are
                                          loaded once, here, not per request
    make_server(config) -> HTTPServer      single-threaded, loopback-bound
    preview_data(doc) -> dict              the honest preview state map for a
                                          document with no resolver yet
    PathError                              a request named something outside
                                          the path allowlist

`src.builder.paths` is the only place a filesystem path is derived, and
`src.builder.store` is the only place one is written. This package imports
nothing that can spawn a process.
"""

from src.builder.paths import PathError
from src.builder.service import BuilderConfig, make_server, preview_data

__all__ = [
    "BuilderConfig",
    "PathError",
    "make_server",
    "preview_data",
]
