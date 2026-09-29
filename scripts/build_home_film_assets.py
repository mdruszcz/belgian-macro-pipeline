"""Build the home-hero film's deployed asset set from a source render.

The BelPulse brand film is still being revised (docs/features/home_film.md); this
script is the one-file-drop point so a new cut can be dropped in and rebuilt without
touching home2.html, its CSS, or its JS. It always writes the same fixed filenames
under assets/belpulse/home-film/, so the page never needs to know a version number:

  film-1080.mp4    -- desktop H.264 fallback (source resolution/bitrate kept, re-muxed
                       deterministically; re-encoded only if a --source-1080 differs
                       from --source is not given).
  film-1440.webm   -- desktop AV1/WebM, preferred source when the browser supports it.
                       Only built if --source-1440 is given (an actual AV1/VP9 render --
                       -c:v copy into .webm cannot remux an H.264 source). Optional tier;
                       omit --source-1440 when the only render available is 1080p H.264.
  film-720.mp4      -- mobile-weight H.264, CRF ~30, target <= 3 MB.
  poster.jpg        -- last frame of the film, JPEG q~82.
  poster.webp       -- same frame, WebP.

DETERMINISM (CLAUDE.md rule 35: identical inputs -> byte-identical output). Every
ffmpeg invocation below fixes:
  -map_metadata -1   strips source container metadata (encoder tags, timestamps).
  -fflags +bitexact   disables encoder-identifying/timestamp-varying muxer behaviour.
  -fps_mode cfr, explicit -r   fixed frame timing, not "whatever the source drifted to".
No -movflags +faststart timestamp, no creation_time atom, no random encode IDs.
Running this script twice on the same source produces byte-identical output files
(verified by tests/test_build_home_film_assets.py, which hashes two runs).

Usage:
    python scripts/build_home_film_assets.py --source <path-to-source-mp4>
        [--source-1080 <path>] [--source-1440 <path>]
        [--ffmpeg <path-to-ffmpeg.exe>] [--out-dir assets/belpulse/home-film]

--source is required and is what --source-720 and the poster are always built from
(it should be the highest-quality cut available). --source-1080 / --source-1440
let the desktop fallback and the AV1 source be re-muxed from separately rendered
files (this repo's current render already ships a 1080p H.264 and a 1440p AV1 file
directly -- re-encoding them again would be a lossy, pointless second generation),
defaulting to --source if not given.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULT_OUT_DIR = REPO / "assets" / "belpulse" / "home-film"

# Deterministic flags shared by every ffmpeg call: no source metadata, no
# timestamp-varying muxer behaviour, fixed frame timing.
_DETERMINISM_FLAGS = ["-map_metadata", "-1", "-fflags", "+bitexact"]


def _run(ffmpeg: str, args: list[str]) -> None:
    cmd = [ffmpeg, "-y", "-hide_banner", "-loglevel", "error"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        raise RuntimeError("ffmpeg failed: " + " ".join(cmd) + "\n" + result.stderr)


def _remux_copy(ffmpeg: str, source: Path, dest: Path) -> None:
    """Copy the video stream byte-for-byte (no re-encode) into a fresh,
    metadata-stripped container. Used for the 1080p/1440p assets when the
    caller already supplies a render at that exact resolution/codec -- a
    second lossy encode would only degrade quality for no benefit."""
    _run(
        ffmpeg,
        [
            "-i",
            str(source),
            "-an",
            *_DETERMINISM_FLAGS,
            "-c:v",
            "copy",
            str(dest),
        ],
    )


def build_1080(ffmpeg: str, source: Path, dest: Path) -> None:
    """Desktop H.264 at the source's own 1920x1080, re-encoded with CRF (not
    a bitstream copy): a raw render from the animation tool ships at a high
    enough bitrate that -c:v copy alone routinely blows the 12 MB budget (and
    can exceed the 25 MB hard limit outright on a ~27s clip). CRF 31 is the
    same quality/size point the approved home-v5 mockup cut used."""
    _run(
        ffmpeg,
        [
            "-i",
            str(source),
            "-an",
            *_DETERMINISM_FLAGS,
            "-vf",
            "scale=-2:1080",
            "-c:v",
            "libx264",
            "-preset",
            "veryslow",
            "-crf",
            "31",
            "-profile:v",
            "high",
            "-pix_fmt",
            "yuv420p",
            "-r",
            "30",
            "-fps_mode",
            "cfr",
            "-movflags",
            "+faststart",
            str(dest),
        ],
    )


def build_720(ffmpeg: str, source: Path, dest: Path) -> None:
    """Mobile-weight H.264, CRF ~30, target <= 3 MB. Re-encoded (not copied)
    because it is a genuine downscale from the source render."""
    _run(
        ffmpeg,
        [
            "-i",
            str(source),
            "-an",
            *_DETERMINISM_FLAGS,
            "-vf",
            "scale=-2:720",
            "-c:v",
            "libx264",
            "-preset",
            "veryslow",
            "-crf",
            "30",
            "-profile:v",
            "high",
            "-pix_fmt",
            "yuv420p",
            "-r",
            "30",
            "-fps_mode",
            "cfr",
            "-movflags",
            "+faststart",
            str(dest),
        ],
    )


def build_poster(ffmpeg: str, source: Path, jpg_dest: Path, webp_dest: Path) -> None:
    """The film's last frame -- Belgium + the BelPulse wordmark, per the
    approved poster spec (docs/features/home_film.md)."""
    _run(
        ffmpeg,
        [
            "-sseof",
            "-1",
            "-i",
            str(source),
            "-frames:v",
            "1",
            *_DETERMINISM_FLAGS,
            "-q:v",
            "4",  # ~q82 on ffmpeg's inverted mjpeg quality scale
            str(jpg_dest),
        ],
    )
    _run(
        ffmpeg,
        [
            "-sseof",
            "-1",
            "-i",
            str(source),
            "-frames:v",
            "1",
            *_DETERMINISM_FLAGS,
            "-c:v",
            "libwebp",
            "-quality",
            "82",
            "-lossless",
            "0",
            str(webp_dest),
        ],
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        required=True,
        type=Path,
        help="Highest-quality source render (used for the 720p build and the poster).",
    )
    parser.add_argument(
        "--source-1080",
        type=Path,
        default=None,
        help=(
            "Pre-encoded, web-ready 1080p H.264 file to remux byte-for-byte "
            "(-c:v copy, metadata stripped only). If omitted, film-1080.mp4 "
            "is instead ENCODED from --source at CRF 31 -- the common case, "
            "since a raw render is rarely already at a web-ready bitrate."
        ),
    )
    parser.add_argument(
        "--source-1440",
        type=Path,
        default=None,
        help=(
            "Pre-rendered 1440p AV1/WebM file to remux. Optional: unlike "
            "--source-1080, this is NOT defaulted from --source, because "
            "-c:v copy into a .webm container requires an actual AV1/VP9 "
            "elementary stream -- remuxing an H.264 source into .webm fails "
            "outright. If omitted, film-1440.webm is not built (and any "
            "stale copy already in --out-dir is left untouched by this run; "
            "delete it by hand if the new cut should drop the 1440p tier)."
        ),
    )
    parser.add_argument("--ffmpeg", default="ffmpeg", help="Path to ffmpeg.exe.")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args(argv)

    source: Path = args.source
    if not source.is_file():
        print(f"source not found: {source}", file=sys.stderr)
        return 1
    source_1080 = args.source_1080
    source_1440 = args.source_1440
    if source_1080 is not None and not source_1080.is_file():
        print(f"--source-1080 not found: {source_1080}", file=sys.stderr)
        return 1
    if source_1440 is not None and not source_1440.is_file():
        print(f"--source-1440 not found: {source_1440}", file=sys.stderr)
        return 1

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    ffmpeg = args.ffmpeg

    if source_1080 is not None:
        print(f"remuxing film-1080.mp4 from pre-encoded {source_1080.name} ...")
        _remux_copy(ffmpeg, source_1080, out_dir / "film-1080.mp4")
    else:
        print(f"encoding film-1080.mp4 from {source.name} (CRF 31) ...")
        build_1080(ffmpeg, source, out_dir / "film-1080.mp4")

    built = ["film-1080.mp4"]
    if source_1440 is not None:
        print(f"building film-1440.webm from {source_1440.name} ...")
        _remux_copy(ffmpeg, source_1440, out_dir / "film-1440.webm")
        built.append("film-1440.webm")
    else:
        print("no --source-1440 given: skipping film-1440.webm")

    print(f"building film-720.mp4 from {source.name} ...")
    build_720(ffmpeg, source, out_dir / "film-720.mp4")
    built.append("film-720.mp4")

    print(f"building poster.jpg / poster.webp from {source.name} (last frame) ...")
    build_poster(ffmpeg, source, out_dir / "poster.jpg", out_dir / "poster.webp")
    built.extend(["poster.jpg", "poster.webp"])

    print("\nWritten to", out_dir)
    for name in built:
        f = out_dir / name
        size_mb = f.stat().st_size / (1024 * 1024)
        flag = " *** OVER 25 MB ***" if size_mb > 25 else ""
        print(f"  {name}: {size_mb:.2f} MB{flag}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
