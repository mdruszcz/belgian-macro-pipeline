# Feature: brand film as the home page hero

Status: approved by the maintainer 2026-09-28. Binaries approved for commit the same day
(rule 12).

Issue: none yet

Branch: feat/home-film-hero

## What this is

home2.html (the canonical homepage -- index.html and home.html both redirect to it) gets a
muted, looping-once brand film as the visual backdrop of its hero band, in place of the
static AI-illustrated background image the hero previously used
(`assets/belpulse/home2/home2-hero-brussels.png`, still used elsewhere on the page). The
goal is a strong first impression that never gets in the way: nothing waits on the video,
nothing plays with sound, nothing goes full-screen, and a returning visitor in the same
browser session sees a still image, not the film again.

## Behaviour

- **Muted, playsinline, never full-screen.** The hero band is ~75vh tall on every viewport;
  the next section always peeks below it. No `requestFullscreen` call exists anywhere on the
  page.
- **Real HTML on top from frame 0.** The existing headline, lead paragraph and "Explore the
  data" CTA are ordinary HTML, positioned above the film layer with a readable scrim
  (a CSS gradient, not baked into the video), so a visitor can read and click immediately --
  nobody waits for the film to load or decide whether to play. The page scrolls normally at
  all times; nothing pins or traps scroll.
- **Plays once per browser session.** A `sessionStorage` flag
  (`belpulse-home-film-played`) is set when the film's `ended` event fires. A later page view
  in the same session shows the poster image (the film's final frame: Belgium plus the
  BelPulse wordmark) with no autoplay, and a small "Replay" button (translated en/fr/nl,
  `homeFilmReplay`) that plays the film again on demand.
- **Pauses out of view, resumes only mid-play.** An `IntersectionObserver` on the hero film
  layer pauses the `<video>` whenever it scrolls out of view and resumes it only if that
  resume is still inside the one automatic session play that has not yet reached `ended`
  (never after a Replay-triggered play has already finished, and never once the poster is
  showing with nothing left to resume).
- **`prefers-reduced-motion` or a slow/Save-Data connection: poster only.** If
  `matchMedia('(prefers-reduced-motion: reduce)')` matches, or
  `navigator.connection.saveData` is true, or `navigator.connection.effectiveType` is
  `slow-2g`/`2g`/`3g`, the page's `initHeroFilm()` returns before ever copying a `<source>`'s
  `data-src` into a live `src` -- no video byte is requested, ever, under these conditions.
- **Mobile weight.** At viewports `<=768px`, only the 720p H.264 source is wired in (the
  1080p/1440p desktop sources are left with no `src`), and on mobile that wiring is deferred
  until the page has finished loading (`load` event, then `requestIdleCallback`/
  `setTimeout`), so the video request cannot compete with or delay the page's own first
  paint -- the poster `<img>` is what actually renders the hero at first paint on every
  device, mobile included.
- **AV1 first, H.264 fallback.** The `<video>` lists a WebM/AV1 `<source>` before the MP4/
  H.264 one on desktop, so a browser that can decode AV1 prefers the smaller file.
- **No JS at all.** With scripting disabled, the `<video>` element is never unhidden (it
  starts `hidden` in the markup) and its `<source>` children carry only `data-src`, never a
  live `src` -- so no video request is ever made. The `<img class="hero-poster">` (a real
  `<img src>`, no JS required) is what paints the hero, together with the always-present
  headline/lead/CTA HTML.

## Assets and the rebuild script

Fixed filenames under `assets/belpulse/home-film/`, so a new film cut is a one-file-drop
that never requires an HTML/CSS/JS change:

| File | Purpose |
|---|---|
| `film-1080.mp4` | Desktop H.264 fallback |
| `film-1440.webm` | Desktop AV1/WebM (preferred where supported) |
| `film-720.mp4` | Mobile-weight H.264 |
| `poster.jpg` | Last frame of the film (JPEG) |
| `poster.webp` | Last frame of the film (WebP) |

`scripts/build_home_film_assets.py --source <path>` regenerates all five from a given
source render (optionally `--source-1080`/`--source-1440` when separately rendered
1080p/1440p files already exist, to remux rather than re-encode them). Every ffmpeg
invocation is deterministic (`-map_metadata -1 -fflags +bitexact`, fixed frame timing via
`-r`/`-fps_mode cfr`) so identical inputs keep producing byte-identical output files (rule
35) -- verified by running the script twice on the same source and hashing the results.

## Film content

The film's map geometry comes from Natural Earth (public domain; see
`docs/data_catalog.md`, "film geometry only, not a statistics source"). The film makes no
data claims of its own -- it is decorative, marked `aria-hidden="true"`, and the section
carries its own text alternative (`homeFilmAlt`, a visually-hidden string) independent of
the film.

## i18n

New interface strings, all in `assets/i18n.js` in en/fr/nl (checked by
`tests/test_i18n.py`'s completeness test): `homeFilmReplay` (the Replay button's visible
text and its `aria-label`), `homeFilmAlt` (the film section's text alternative).

## Tests

- `tests/test_home_film_hero.py` -- static: the film/poster assets exist and are each
  <=25 MB, home2.html references every source and the poster, the `<video>` is
  muted/playsinline/`preload="none"` with no `autoplay` attribute, the film layer is
  `aria-hidden` and the page never calls into the Fullscreen API, the hero band is `75vh`,
  the headline/lead/CTA sit above the film layer (z-index), the `<source>` elements carry
  only `data-src` in markup, the page still has exactly one `<h1>`, and the i18n keys exist
  in all three languages.
- `tests/test_home_film_hero_browser.py` -- Playwright/Chromium (skips cleanly where
  unavailable): a first visit autoplays muted; a second navigation in the same
  session shows the poster with no autoplay and a visible Replay button; with
  `reduced_motion='reduce'` no film request is ever made; with `navigator.connection.saveData`
  no film request is ever made; at a 390px viewport only the 720p source is wired in; the
  CTA is clickable immediately (t=0), before the video has loaded.
- `tests/test_charts_tooltip.py` (existing) re-run unchanged to confirm the hero's absolute-
  coordinate hover tests still pass after the layout change.

## What this batch did not do

- Did not touch the film's own content or story -- the render is supplied separately and is
  still being revised; this batch only integrates whatever the current render is, in a way
  that a later drop-in requires no code change.
- Did not add captions/subtitles inside the video (there is no dialogue or on-screen text
  carrying a data claim to caption).
- Did not change `assets/belpulse/home2/home2-hero-brussels.png`'s other uses on the page
  (unrelated to this batch's scope).
