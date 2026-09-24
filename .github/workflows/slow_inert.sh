#!/usr/bin/env bash
# Can this change affect the `slow` tests (build_staging_db, committed-store
# consistency, exports, offload, orchestration parity, validation)? Writes
# `slow_inert=true|false` to $GITHUB_OUTPUT (or stdout when run by hand).
# Consulted by ci.yml's generated-site tier alongside site_inert.sh: when
# BOTH are true, the tier skips its tests entirely; the exact combination is
# decided in ci.yml, not here.
#
# "Slow-inert" means every changed path is in the allowlist below: paths the
# slow tests do not read. That allowlist is: docs/**, *.md, assets/**
# (browser JS/CSS/i18n -- the slow tests never import it), a top-level
# *.html page, and a changed test file that is NOT itself slow-marked
# (decided by grepping ITS CONTENT for a slow marker, e.g.
# `pytest.mark.slow` or a `pytestmark` line mentioning slow -- never a
# hardcoded path list, so a new slow test file is never wrongly skipped).
# tests/conftest.py is never inert: every test, slow or not, depends on it.
#
# Everything else -- data/, config/, scripts/, src/, orchestration/,
# migrations/, Makefile, pyproject, requirements, workflow files -- is not
# inert. Any doubt (no usable base, a first push, an empty diff) resolves to
# false: a wrongly skipped run could merge a broken pipeline; an unnecessary
# run costs minutes. Same base-commit logic as site_inert.sh/docs_only.sh.
set -u

out="${GITHUB_OUTPUT:-/dev/stdout}"
event="${GITHUB_EVENT_NAME:-}"
base="${SLOW_INERT_BASE:-}"

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
  echo "slow_inert=false (no usable base commit: '${base:-none}')" >&2
  echo "slow_inert=false" >> "$out"
  exit 0
fi

changed="$(git diff --name-only "$base" HEAD)"
if [ -z "$changed" ]; then
  echo "slow_inert=false (empty diff)" >&2
  echo "slow_inert=false" >> "$out"
  exit 0
fi

# Paths the slow tests never read, regardless of content.
always_inert='^docs/|\.md$|^assets/|^[^/]+\.html$'

# A slow-marker grep run against a changed test file's CONTENT at HEAD, not
# its path. Matches `@pytest.mark.slow`, `pytest.mark.slow(...)`, or a
# `pytestmark` line mentioning slow (module-level marker application).
is_slow_marked() {
  git show "HEAD:$1" 2>/dev/null | grep -qE 'pytest\.mark\.slow|pytestmark[^#]*slow'
}

relevant=""
while IFS= read -r path; do
  [ -z "$path" ] && continue
  if [ "$path" = "tests/conftest.py" ]; then
    relevant="$relevant$path
"
    continue
  fi
  if printf '%s' "$path" | grep -qE "$always_inert"; then
    continue
  fi
  # Not in the always-inert set. Only a non-slow-marked test file is inert;
  # anything else (data/, config/, scripts/, src/, orchestration/,
  # migrations/, Makefile, pyproject, requirements, workflow files, and any
  # test file that IS slow-marked or was deleted so it can't be read) counts.
  case "$path" in
    tests/*.py)
      if git cat-file -e "HEAD:$path" 2>/dev/null && ! is_slow_marked "$path"; then
        continue
      fi
      ;;
  esac
  relevant="$relevant$path
"
done <<EOF
$changed
EOF

if [ -n "$relevant" ]; then
  echo "slow_inert=false -- paths the slow tests can read:" >&2
  printf '%s' "$relevant" | head -20 >&2
  echo "slow_inert=false" >> "$out"
else
  echo "slow_inert=true -- no changed path feeds the slow tests:" >&2
  printf '%s\n' "$changed" | head -20 >&2
  echo "slow_inert=true" >> "$out"
fi
