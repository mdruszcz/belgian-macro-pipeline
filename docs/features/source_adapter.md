# Feature: Generic source-adapter system

Status: draft
Issue: (Block D — Generic source-adapter system, docs/steps)
Branch: feat/source-adapter-base

## Problem

`belgian_macro_db.py` has three fetcher classes — `NBBFetcher`, `DBnomicsFetcher`, `FPBFetcher`
— each a bare `@staticmethod`, each reimplementing the same three things independently: an HTTP
GET with a 30-second timeout, a `raise_for_status()`, and per-code error handling in `fetch_all()`
that catches whatever the fetcher raises. None of them retry a transient failure, none of them
cache the raw response before parsing it, and none of them write to `fetch_runs` — the table
`migrations/001_core_schema.sql` built specifically for this ("when did source X last succeed").
`fetch_runs` is written today only by `scripts/sync_to_canonical.py`, which runs *after*
`fetch_all()` with no network calls of its own — so a network failure, an HTTP 500, or a parser
silently dropping half a response leaves no trace in the table meant to detect exactly that.

Every future source (Statbel for Block F, WalStat for Block O, and whatever Block E's
catalogue selects) costs a full day of reimplementing retry/caching/logging instead of an hour of
subclassing, if the interface is right. Getting it wrong now compounds with every source added
after it.

## Goal

- One abstract base class (`DataSource`) that every adapter subclasses, providing: an HTTP GET
  with retry, raw-response caching to `data/raw/{source_id}/{date}/`, and `fetch_runs` logging —
  written once, inherited by all three (and every future) adapter.
- `NBBFetcher` → `NBBSource`, `DBnomicsFetcher` → `EurostatSource`, `FPBFetcher` → `FPBSource`,
  each with **identical parsing behaviour** to today — this is a refactor of the fetch/cache/log
  scaffolding around each adapter, not a change to what it extracts from a response.
- A contract test proves the claim above for the two adapters that share a contract (see Non-goals).
- Every adapter fetch call writes a `fetch_runs` row (started/finished, status, rows read/written,
  HTTP status, message) — new observability, not a replacement for the existing
  `legacy_fetch_log`/`db.log_fetch()` path, which `fetch_all()`'s tests already pin down.

## Non-goals

- **Forcing FPB into the same output shape as NBB/Eurostat.** `FPBFetcher` returns
  `{institution, indicator, year, value, updated_at}` — genuinely different from
  `{period, value, obs_status}`, because it feeds the `forecasts` table (multiple institutions ×
  multiple indicators per fetch), not `legacy_observations` (one indicator per fetch). Coercing
  FPB into the time-series shape would mean reworking `upsert_forecasts` and the `forecasts`
  table — a real behaviour change this block does not ask for, and risk to a working part of the
  pipeline for no stated benefit. **Decision: two adapter contracts, not one.**
  `TimeSeriesSource` (NBB, Eurostat/AMEco) returns the period/value/obs_status shape and is
  where the contract test applies; `ForecastSource` (FPB) keeps its own shape. Both share the
  same base class for fetch/cache/retry/log.
- **Uploading cached raw responses as CI artifacts.** `data/raw/{source_id}/{date}/` is local,
  gitignored (matching the existing `/data/raw/**` pattern already covering
  `data/raw/statbel/`), and vanishes with the ephemeral GitHub Actions runner between daily runs.
  It solves the stated problem — "when a parse breaks six months from now, you need the exact
  bytes the source returned that day" — for a developer running the pipeline locally to
  reproduce a bug; making it durable across CI runs is a separate, later decision (it would touch
  `daily_fetch.yml`, which this block does not).
- **Touching `sync_to_canonical.py`'s existing `fetch_runs` writes.** That script already writes
  `fetch_runs` rows to get a `fetch_run_id` for the observations it inserts — a real, working FK
  relationship. This block's adapters write *additional* `fetch_runs` rows describing the actual
  network fetch, at the point the fetch happens rather than as a same-day proxy computed from
  `legacy_observations` with no network involved. Two rows per source per day (one from the live
  fetch, one from the sync step) is a feature, not a duplication: they describe two different
  processing stages, and only the fetch-stage row can ever show a network failure.
- **Adding new sources.** This is the interface Statbel/WalStat will implement later; it adds no
  new source itself.
- **Rate limiting.** Nothing in the current pipeline requests fast enough to need one — the
  daily workflow fetches ~26 URLs once a day. Retry-on-failure is in scope; throttling proactive
  request rate is not, until a source's terms require it (tracked in `docs/data_catalog.md` if so).

## Proposed approach

### Interface

`src/fetchers/base.py`:

```python
class DataSource(ABC):
    source_id: str    # matches sources.source_id, e.g. "nbb", "dbnomics_eurostat"
    adapter: str       # matches sources.adapter / fetch_runs.adapter, e.g. "nbb", "dbnomics", "fpb"
    raw_extension: str # "csv" | "json" | "xlsx" -- the cached file's extension

    def fetch(self, url: str, *, cache_key: str, conn: sqlite3.Connection | None = None,
              **parse_kwargs) -> list[dict]:
        """Template method: GET-with-retry -> cache raw -> parse -> log fetch_runs -> return rows."""

    @abstractmethod
    def _parse(self, raw: bytes, **kwargs) -> list[dict]: ...
```

`fetch()` is concrete and shared; only `_parse()` varies per adapter. It:

1. Records `started_at`.
2. GETs `url` with up to 3 attempts, exponential backoff (1s/2s/4s), retrying on
   `requests.exceptions.RequestException` (covers `Timeout`, `ConnectionError`) and on HTTP 5xx.
   **Does not retry 4xx** — a bad URL or malformed request will not succeed on attempt 2, and
   retrying it only delays the failure being surfaced.
3. On success, writes the raw response bytes to
   `data/raw/{self.source_id}/{YYYY-MM-DD}/{cache_key}.{self.raw_extension}` — one file per
   `(source, day, cache_key)`, overwritten on a same-day re-run. `cache_key` is caller-supplied
   (the indicator code for NBB/Eurostat calls, a fixed constant for FPB's single call) because
   `fetch()` is invoked once per indicator for the time-series sources, and indicator codes are
   the only thing that disambiguates otherwise-identical `(source_id, date)` cache paths.
4. Calls `self._parse(raw, **parse_kwargs)` — everything adapter-specific lives here, unchanged
   from the current `NBBFetcher.fetch`/`DBnomicsFetcher.fetch`/`FPBFetcher.fetch` bodies.
5. Records `finished_at`, and if `conn` is given, inserts one `fetch_runs` row
   (`source_id`, `adapter`, `started_at`, `finished_at`, `status`, `rows_read`, `rows_written`,
   `http_status`, `message`) — `rows_read` is `len(raw_rows_before_filtering)` where the parser
   exposes that count, else equal to `rows_written`; `status` is `'ok'` on success, `'error'` with
   the exception message on failure. **Logging failure must never mask a fetch failure**: the
   `fetch_runs` insert happens in a `try/except` of its own around the outer exception handling,
   so a `sqlite3` error while logging cannot swallow the real HTTP/parse error that triggered it.
6. Re-raises on failure (after logging) rather than swallowing it — `fetch_all()`'s existing
   `try/except` per source, and its exact `all_ok`/`db.log_fetch("ERROR", ...)` contract
   (asserted by `tests/test_fetch_all.py`), is unchanged.

`TimeSeriesSource(DataSource)` fixes the contract `_parse` must satisfy: return
`list[dict]` where every dict has exactly `{"period": str, "value": float, "obs_status": str}`.
`ForecastSource(DataSource)` fixes FPB's: `{"institution": str, "indicator": str, "year": str,
"value": float | None, "updated_at": str}`.

### Adapters

- **`NBBSource(TimeSeriesSource)`** — `source_id="nbb"`, `adapter="nbb"`, `raw_extension="csv"`.
  `_parse` is `NBBFetcher.fetch`'s existing body verbatim (SDMX-CSV `csv.DictReader`, per-period
  dedup keeping the last row, silent skip of blank/unparseable rows), with the `requests.get`
  call removed — that is now the base class's job.
- **`EurostatSource(TimeSeriesSource)`** — `source_id` is **not** fixed per class instance: the
  same adapter code serves both `dbnomics_eurostat` and `dbnomics_ameco` sources (identical
  `adapter: dbnomics` in `config/sources/*.yaml`, different `source_id`), so `source_id` is an
  `__init__` parameter, not a class attribute, defaulting from the `SOURCES[code]["source_agency"]`
  the caller already has. `_parse` is `DBnomicsFetcher.fetch`'s existing body verbatim (DBnomics
  JSON shape, `< "2008"` filter, `index_2010` rebasing).
- **`FPBSource(ForecastSource)`** — `source_id="fpb"`, `adapter="fpb"`, `raw_extension="xlsx"`.
  `_parse` is `FPBFetcher.fetch`'s existing body verbatim (temp-file XLSX read via `openpyxl`,
  positional column layout, `_parse_value`'s comma-decimal/missing-marker handling) — the only
  difference is the raw bytes are now the base class's cached file's content rather than a
  fresh `tempfile` written inside the method; `openpyxl.load_workbook` still needs a path or
  file-like object, so `_parse` opens the cached file directly instead of re-downloading into a
  new temp file.

### `fetch_all()` changes

Constructs one `NBBSource`/`EurostatSource` per indicator (or reuses instances — they are
stateless besides `source_id`/`adapter`, so one instance per distinct `source_id` is enough) and
one `FPBSource`, passing `db.conn` as `conn` so every call logs to `fetch_runs` in the same
transaction the legacy tables are already using. The `if meta["type"] == "nbb"` dispatch stays —
this block does not change *which* adapter handles which source, only what each adapter is built
from.

## Data / schema changes

None. `fetch_runs` (`migrations/001_core_schema.sql`) already has every column this needs.

## New data sources

None introduced by this block.

## Tests

- `tests/test_fetchers_base.py` — retry behaviour (fails twice then succeeds; exhausts retries
  and raises; does not retry on 4xx), raw caching (file written to the expected path, overwritten
  on a second call same day), `fetch_runs` logging (row written on success and on failure;
  logging failure does not mask the real exception).
- `tests/test_nbb_source.py`, `test_eurostat_source.py`, `test_fpb_source.py` — HTTP-mocked
  (`requests_mock` or monkeypatched `requests.get`) tests of the full `fetch()` path for each
  adapter, asserting output identical to what the current bare-function tests only exercise as
  pure sub-functions (`_rebase_to_2010`, `_parse_value`) — this closes the gap the current test
  suite has, where no test exercises `NBBFetcher.fetch`/`DBnomicsFetcher.fetch`/`FPBFetcher.fetch`
  against a mocked response at all.
- `tests/test_source_contract.py` — parametrized over `NBBSource`/`EurostatSource` (not FPB, see
  Non-goals): given a fixture raw response for each, asserts the returned rows have exactly the
  keys `{period, value, obs_status}` with `str`/`float`/`str` types.
- Existing tests updated, not replaced: `tests/test_fetch_all.py`'s assertions about `all_ok`,
  `db.log_fetch` calls, and `"FPB_FORECASTS"` as the FPB log code must still pass unchanged —
  they test the orchestration contract, which this block does not alter.

## Assumptions and open questions

- **Retry count and backoff (1/2/4s, 3 attempts) are a starting default**, not tuned against any
  observed failure pattern from NBB/DBnomics/FPB — none of the three has ever needed a retry in
  this pipeline's history. Revisit if a real transient-failure pattern shows up in `fetch_runs`.
- **`rows_read` vs `rows_written`**: NBBSource reports both — the SDMX response can contain more
  raw rows than survive the per-period dedup (`seen: dict[str, dict]`, last-write-wins), and the
  two diverging is itself useful signal (a revision landed, or a duplicate period appeared).
  EurostatSource/FPBSource duplicate `rows_written` into `rows_read` rather than fabricate a
  distinct number for shapes where computing one cheaply isn't as direct.
- **`http_status` on a retry**: only the *final* attempt's status is recorded — recording every
  attempt would need a richer `fetch_runs` row shape (an array, not a single int column), which is
  out of scope for what the schema already provides.
- **`sources.source_id` and `config/sources/*.yaml`'s `source_id` disagree, and this block does
  not reconcile them.** Discovered while wiring `fetch_runs.source_id` (a real FK column):
  `config/sources/dbnomics_eurostat.yaml`/`dbnomics_ameco.yaml` declare `source_id:
  dbnomics_eurostat`/`dbnomics_ameco`, but the canonical `sources` table — populated separately
  by `scripts/sync_to_canonical.py` via `source_id_for(agency)` — actually holds `eurostat`/
  `ameco_ec` (`source_id_for("Eurostat")`, not the config file's own declared id). `fetch_runs`
  therefore uses `source_id_for(meta["source_agency"])` (the value that is *actually* populated
  in `sources` today), not the config file's `source_id` field, to avoid inserting rows an FK
  join could never resolve. Fixing the underlying Block B/canonical-sync naming mismatch is a
  separate, pre-existing issue this block did not introduce and does not fix.
- **`fpb` is not in the canonical `sources` table at all** (FPB indicators are excluded from
  `is_canonical_eligible`, so `sync_to_canonical.py` never inserts a `sources` row for it).
  `FPBSource`'s `fetch_runs` rows therefore reference a `source_id` with no corresponding `sources`
  row. This does not raise today because `MacroDatabase`'s connection does not set `PRAGMA
  foreign_keys=ON` (unchanged by this block), but it is worth recording as a real gap rather than
  relying on the pragma being off to stay silent.

## Rollout / risks

- **The refactor must not change what ends up in `legacy_observations`/`forecasts`.** Verified for
  real, not just by unit test: `develop`'s `belgian_macro_db.py` (via a `git worktree`) and this
  branch's were each run live with `--fetch --export csv --export json` against separate DB
  copies. Every NBB/DBnomics source succeeded identically on both; FPB failed identically on both
  (`403 Forbidden` from `plan.be` — a pre-existing block unrelated to this refactor, confirmed
  present before it too). The two `belgian_macro_export.csv` outputs are **row-for-row identical
  apart from the `fetched_at` timestamp** (2027 data rows, compared as a set ignoring that one
  column) — this is CONTROL D's own diff, executed against live data.
- **A real retry bug was caught by its own test, before this ever ran live**:
  `resp.raise_for_status()` raises `requests.exceptions.HTTPError`, itself a `RequestException`
  subclass — an earlier version of `_get_with_retry` caught that in the same `except` block as
  connection-level errors, so a 404 was retried 3 times, directly contradicting "never retry a
  client error" a few paragraphs above it in this same document.
  `test_fetch_does_not_retry_a_client_error` failed immediately and named the exact problem;
  fixed by separating the connection-level try/except (wraps only `requests.get()`) from the
  HTTP-status retry decision (a plain check on `resp.status_code`, never inside a `except
  RequestException`). Recorded here because it is exactly the kind of thing "no behaviour change"
  is supposed to catch, and it did.
- **New `fetch_runs` rows land as new evidence that a source is failing.** If a source has been
  silently degraded (e.g. slowly returning fewer rows) this makes it visible for the first time
  via `rows_read`/`rows_written` — a good outcome, but means `fetch_runs` may show a "problem"
  that was already present and simply unmeasured before. Not a regression; worth stating so it
  isn't mistaken for one.
