#!/usr/bin/env bash
# Is this CI run's change docs-only? Writes `docs_only=true|false` to
# $GITHUB_OUTPUT (or stdout when run by hand). See ci.yml's own header
# comment for why each tier consults this instead of a workflow-level
# `paths:` filter: a job that never starts is "skipped", and the `test`
# gate rightly treats a skipped tier as a failure -- so the tier itself
# has to run, look, and succeed while doing nothing.
#
# "Docs-only" means every changed path is under docs/ or is a Markdown
# file anywhere. Anything else -- code, config, data, tests, workflows,
# HTML, assets -- is a real change and gets the full run. Any doubt (no
# base to diff against, a first push to a new branch, git unable to
# resolve the base) resolves to false: an unnecessary full run costs a few
# minutes, a wrongly skipped one could merge broken code.
set -u

out="${GITHUB_OUTPUT:-/dev/stdout}"
event="${GITHUB_EVENT_NAME:-}"
base="${DOCS_ONLY_BASE:-}"

if [ -z "$base" ]; then
  case "$event" in
    pull_request)
      # The PR's base branch tip, as GitHub recorded it for this run.
      base="$(python3 -c 'import json,os; print(json.load(open(os.environ["GITHUB_EVENT_PATH"]))["pull_request"]["base"]["sha"])' 2>/dev/null || true)"
      ;;
    push)
      base="$(python3 -c 'import json,os; print(json.load(open(os.environ["GITHUB_EVENT_PATH"]))["before"])' 2>/dev/null || true)"
      ;;
  esac
fi

zeros="0000000000000000000000000000000000000000"
if [ -z "$base" ] || [ "$base" = "$zeros" ] || ! git cat-file -e "$base^{commit}" 2>/dev/null; then
  echo "docs_only=false (no usable base commit: '${base:-none}')" >&2
  echo "docs_only=false" >> "$out"
  exit 0
fi

changed="$(git diff --name-only "$base" HEAD)"
if [ -z "$changed" ]; then
  echo "docs_only=false (empty diff)" >&2
  echo "docs_only=false" >> "$out"
  exit 0
fi

if printf '%s\n' "$changed" | grep -qvE '^docs/|\.md$'; then
  echo "docs_only=false -- non-docs paths changed:" >&2
  printf '%s\n' "$changed" | grep -vE '^docs/|\.md$' | head -20 >&2
  echo "docs_only=false" >> "$out"
else
  echo "docs_only=true -- only docs changed:" >&2
  printf '%s\n' "$changed" >&2
  echo "docs_only=true" >> "$out"
fi
