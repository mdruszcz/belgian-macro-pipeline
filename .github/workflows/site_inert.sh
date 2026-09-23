#!/usr/bin/env bash
# Can this change affect the generated site? Writes `site_inert=true|false`
# to $GITHUB_OUTPUT (or stdout when run by hand). Consulted by ci.yml's
# generated-site tier: when true, the tier skips its `generated_site` sweeps
# (one case per route or page -- the slow part) but STILL runs every `slow`
# test, which is where the store, validation and export checks live.
#
# "Site-inert" means every changed path is in the allowlist below: paths
# that no page generator reads. Anything else -- page code, exporters,
# templates, assets, HTML, public/data, config/pages or indicators, and
# data/ (export_local_pages.py reads data/belgian_macro.db) -- gets the full
# tier. Any doubt (no usable base, a first push, an empty diff) resolves to
# false: a wrongly skipped sweep could merge a broken page; an unnecessary
# run costs thirteen minutes. Same base-commit logic as docs_only.sh.
set -u

out="${GITHUB_OUTPUT:-/dev/stdout}"
event="${GITHUB_EVENT_NAME:-}"
base="${SITE_INERT_BASE:-}"

if [ -z "$base" ]; then
  case "$event" in
    pull_request)
      base="$(python3 -c 'import json,os; print(json.load(open(os.environ["GITHUB_EVENT_PATH"]))["pull_request"]["base"]["sha"])' 2>/dev/null || true)"
      ;;
    push)
      base="$(python3 -c 'import json,os; print(json.load(open(os.environ["GITHUB_EVENT_PATH"]))["before"])' 2>/dev/null || true)"
      ;;
  esac
fi

zeros="0000000000000000000000000000000000000000"
if [ -z "$base" ] || [ "$base" = "$zeros" ] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
  echo "site_inert=false (no usable base commit: '${base:-none}')" >&2
  echo "site_inert=false" >> "$out"
  exit 0
fi

changed="$(git diff --name-only "$base" HEAD)"
if [ -z "$changed" ]; then
  echo "site_inert=false (empty diff)" >&2
  echo "site_inert=false" >> "$out"
  exit 0
fi

# Paths no page generator reads. The generated-site tests themselves
# (tests/pages, tests/site, tests/conftest.py, test_export_local_pages.py)
# are deliberately NOT here: changing a sweep must run it.
inert='^docs/|\.md$|^src/fetchers/|^src/validation/|^scripts/(sync|fetch)_[^/]+\.py$|^orchestration/|^config/sources/|^config/stores\.yaml$|^\.github/workflows/(daily_fetch\.yml|site_inert\.sh|docs_only\.sh)$|^tests/'
not_inert='^tests/(pages|site)/|^tests/conftest\.py$|^tests/test_export_local_pages\.py$'

relevant="$(printf '%s\n' "$changed" | grep -vE "$inert"; printf '%s\n' "$changed" | grep -E "$not_inert")"
if [ -n "$relevant" ]; then
  echo "site_inert=false -- paths that can affect the generated site:" >&2
  printf '%s\n' "$relevant" | head -20 >&2
  echo "site_inert=false" >> "$out"
else
  echo "site_inert=true -- no changed path feeds a page generator:" >&2
  printf '%s\n' "$changed" | head -20 >&2
  echo "site_inert=true" >> "$out"
fi
