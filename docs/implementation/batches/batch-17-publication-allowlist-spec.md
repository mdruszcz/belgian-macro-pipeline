# Batch 17 — publication allowlist (pre-implementation specification, revision 1)

```
Batch: 17 (new; maintainer-directed, 2026-09-07)
Base commit: TBD — after 12b commits AND chore/playwright-ci-gate (368176f1) is merged
Branch: feat/publication-allowlist
Status: SPEC — not yet handed off
Chain: lead -> builder -> auditor -> lead's final decision (deploy-path and security-sensitive)
```

## Maintainer step this batch cannot complete on its own

There is **no Pages workflow** in `.github/workflows/`. Pages serves branch `develop` at path
`/` with `build_type: legacy` — configured in repository *settings*, not in code. If this batch
moves to an artifact deploy, **the maintainer must flip the Pages source to "GitHub Actions" in
the GitHub UI himself.** No agent can do it and no test in this repository can observe it.

Until that flip happens the workflow runs, goes green, and the site keeps serving the repository
root. **The batch is not done when the workflow merges.** It is done when the setting is flipped
and a fetch of a known-excluded URL against the live site returns 404, recorded as a real
observation in the batch report.

## Objective

GitHub Pages currently serves **this repository's root**. Everything tracked in git is
therefore live on the public internet the moment it is pushed. The maintainer's decision:
**keep committing drafts — exclude them from the published site.** Draft history in git and
cross-machine work are wanted; gitignoring drafts was considered and rejected. So the deploy
step is what changes: Pages must publish an **allowlist of what belongs on the public site**
instead of serving the repository root.

## What this allowlist can and cannot guarantee — read this before writing a test

**Revision 1 of this spec was wrong about the reach of the fix, and the correction matters more
than the original finding.** It treated the allowlist as a way to stop `data/belgian_macro.db`
and other committed files being downloadable. It is not, and it cannot be.

**The repository is public.** Verified directly: `data/belgian_macro.db` returns HTTP 200,
18,161,664 bytes, at **both**

- `https://mdruszcz.github.io/belgian-macro-pipeline/data/belgian_macro.db` (Pages), and
- `https://github.com/mdruszcz/belgian-macro-pipeline/raw/develop/data/belgian_macro.db` (github.com).

A Pages allowlist closes the first door only. Anything committed to a public repository stays
downloadable from github.com regardless of what Pages publishes. An artifact deploy would have
produced a green build, a passing exclusion test, and an entirely unchanged exposure — the exact
silent-success failure this spec warns about, one level up, and shipped by the very test meant
to prevent it.

**So the guarantee this batch makes is about the published site's surface, not about secrecy.**
Every test, comment and report sentence must be phrased that way. "Not reachable in the built
output" is true and checkable. "Not public" is false, and writing it would leave the next reader
believing in a protection that does not exist.

**The maintainer has ruled on the exposure itself: the public repository and public data are
intentional.** This is an open-data project. The database, CSV exports, `src/`, `config/`,
`tests/`, `scripts/` and `migrations/` being fetchable is a **reviewed and accepted risk** as of
2026-09-07, not an open finding, and not this batch's problem to solve.

What remains, and what this batch actually does: **drafts stay in git, and the published site
excludes them.** That is what was asked for originally, and it is achievable.

## What reconnaissance found

Measured against the actual git index — still useful for building the allowlist, now read as
"what belongs on the site", not "what must be hidden":

| Tracked directory | Files | Belongs on the public site? |
|---|---|---|
| `local/` | 1,696 | **Yes** — the crawler-visible commune pages |
| `public/` | 623 | **Yes** — the published payloads |
| `assets/` | 11 | **Yes** |
| root `*.html` (`index`, `dashboard`, `communes`, `map`, `local`, `all_data`, `about`, `home`, `commune`) + `shared-styles.css` | 10 | **Yes** |
| `config/` | 97 | No — includes `config/pages/` drafts, the reason for this batch |
| `data/` | 22 | No — belongs in git, not on the site's URL surface |
| `src/`, `scripts/`, `tests/`, `migrations/`, `docs/`, `.claude/`, `.github/` | 235 | No |

The right-hand column means "should this be part of the website's URL surface", and nothing
stronger. Everything in the bottom three rows remains fetchable from github.com either way, by
the maintainer's deliberate choice. Excluding them from the published site is site hygiene — a
smaller, comprehensible public surface, and a deploy that publishes what was designed rather
than whatever happens to be committed. It is not a confidentiality control and must never be
described as one.

**Second finding: there is no route inventory, and no test asserts that any URL resolves.**
Rule 31 ("every existing canonical and legacy URL remains valid after the redesign") has no
mechanical baseline anywhere in this repository — the only `legacy` references in `tests/` are
about database tables and adapters, not URLs. So the acceptance baseline this batch needs does
not exist yet and must be built **first**, before the deploy changes. An allowlist shipped
against no inventory is a guess.

## The two constraints that are the whole risk

**1. A mistake in the allowlist puts drafts back onto the published site silently.** No error, no
failed build, just a reachable URL. Therefore the acceptance criterion is *not* "drafts are
excluded". It is: **a test fails if `config/pages/*/draft.json`, or anything else outside the
allowlist, is reachable in the built output.** The guarantee has to be mechanical, not
procedural — and it is a guarantee about the built output, which is the only thing a test here
can actually observe.

**Default-deny, not deny-list.** The allowlist enumerates what ships; everything else is
excluded because it was never included. A deny-list would require someone to remember to add
each new draft-shaped file, and that memory is exactly what fails. A new file added later must
be excluded *automatically*, and a new file that legitimately belongs on the site must fail a
test until it is explicitly allowed — that direction of failure is the safe one.

**2. Rule 31 is the constraint most likely to be broken here, and it is now the batch's centre of
gravity.** An allowlist that misses an asset directory or a legacy redirect breaks live URLs
silently — and since the draft exclusion is site hygiene rather than a confidentiality fix, a
broken URL is comfortably the more serious of the two failures this batch can cause. A visitor
hitting a 404 on a page that worked yesterday is a regression users report; a draft on the site's
URL surface is one nobody notices. Both matter; only one is self-reporting.

**The route inventory is the most valuable thing in this batch.** Rule 31 having no mechanical
enforcement across 1,695 commune pages in three languages is a genuine gap regardless of whether
the deploy ever changes. It lands first, and it is worth keeping even if everything else here is
reverted.

## Included work

1. **A route inventory, as a test, committed before the deploy changes.** Every canonical and
   legacy URL the site serves today, derived from what actually exists rather than from memory:
   the 9 root HTML pages, the `local/{nis}/`, `local/{nis}/fr/`, `local/{nis}/nl/` triples for
   every commune (1,695 pages plus `local/sitemap.xml`), the `public/data/**` payload paths the
   pages fetch, and `assets/**`. The sitemap at `local/sitemap.xml` declares the canonical URL
   shape — `https://mdruszcz.github.io/belgian-macro-pipeline/local/{nis}/{,fr/,nl/}` — and is
   the natural cross-check: **every URL the sitemap declares must be reachable in the built
   output.** A sitemap that advertises a URL the deploy no longer publishes is a rule 31 breach
   that also poisons search indexing.
2. **The allowlist itself**, default-deny, in one place, with each entry carrying a one-line
   reason. One file, readable by a non-developer, not scattered across a workflow.
3. **The deploy change.** Pages stops serving the repository root and starts publishing a built
   artefact containing only allowlisted paths.
4. **The mechanical guarantee**: tests that fail on a leak and on a missing route (see Tests).

## The architecture decision

An artifact-based deploy: a workflow that assembles the allowlisted files into a directory and
publishes that as the Pages artifact, replacing `build_type: legacy` serving of branch `develop`
at `/`. The manual settings flip this depends on is stated at the top of this spec, because it is
the single most likely way the work fails in practice and it must not be discovered at the end.

## Explicit exclusions

- **Do not gitignore drafts, or move `config/pages/` out of git.** That was considered and
  rejected by the maintainer; drafts stay committed.
- Do not delete `data/belgian_macro.db` from git, rewrite history, or make the repository
  private. The public repository and public data are the maintainer's deliberate choice — this
  is an open-data project — and are a reviewed, accepted risk, not a problem to solve here.
- Do not describe any part of this work as making a committed file private, in a test name, a
  comment, a commit message or the batch report. It does not, and a claim like that is how the
  next person stops checking.
- Do not scope in licence or attribution work. See the note below.
- Do not change any page's content, any exporter, any payload, or any URL. This batch changes
  **what is published**, never **what is generated**. If a route needs to change to make the
  allowlist work, that is a finding to escalate, not a change to make.
- Do not touch `src/fetchers/`, `src/analytics/`, `resolve_geo()`, or the database schema.
- Do not fold in any part of Batch 12, and do not start until 12b is committed and
  `chore/playwright-ci-gate` (368176f1) is merged.

## Acceptance criteria

1. `config/pages/*/draft.json` is not present in the built output, and a test **fails** if it is.
2. `data/**`, `config/**`, `src/**`, `tests/**`, `scripts/**`, `migrations/**`, `docs/**` and
   `.claude/**` are not present in the built output — covered by the same default-deny test
   rather than by eight special cases. This is about the site's URL surface; those files stay
   fetchable from github.com and that is intended.
3. A file added anywhere outside the allowlist is excluded automatically, proven by a test that
   adds one and asserts its absence.
4. Every URL in the route inventory resolves in the built output. Every URL declared in
   `local/sitemap.xml` resolves in the built output.
5. The 9 root pages, all 1,695 commune pages in all three languages, `public/data/**` and
   `assets/**` are present and byte-identical to what the repository holds.
6. Verified against the **live site** after the Pages setting is flipped: a known-excluded URL
   returns 404, and a sample of canonical URLs across all three languages still return 200.

## Tests required

- **Exclusion test, default-deny.** Build the output, walk it, assert every path matches the
  allowlist. The assertion is over the *built tree*, not over the allowlist file — a test that
  reads the allowlist and confirms the allowlist agrees with itself proves nothing. Name it for
  what it checks (reachability in the built output), never "is private".
- **A deliberately planted file** outside the allowlist is absent from the build.
- **Route inventory test**: every inventoried URL resolves; parameterised over communes rather
  than spot-checked, since 1,695 pages is exactly the scale at which a sampled check misses a
  systematic gap.
- **Sitemap agreement**: every `<loc>` in `local/sitemap.xml` is reachable.
- Full suite green, and the browser tests genuinely running in CI (Playwright landed in
  `368176f1`; a skipped run is a failed gate now, not an accepted one).

## Rollback point

The deploy is the risky part and it is also the easy rollback: revert the workflow commit and
set the Pages source back to "deploy from a branch". The site returns to serving the repository
root within one deploy cycle. Nothing about the site's *content* changes in this batch, so there
is no data or payload rollback to perform. The route inventory test is independently safe to keep
whatever happens to the deploy, and should be kept.

## Audit scope

Regression first, then the exclusion property. Specifically **not** a data-provenance audit —
no figure, payload or binding changes here.

- **Regression against the route inventory** is the dimension most likely to produce a P0, and
  the reason this batch is audited at all.
- **Exclusion property**: confirm default-deny holds against paths the implementer did not think
  of (dotfiles, nested `config/pages/*/versions/*.json`, symlinks, case variations). Judge the
  claim being made, not just the code: a test or report sentence asserting that a committed file
  has been made private is **wrong** and is itself a finding, because the repository is public
  and those files remain fetchable from github.com by design.
- **Regression against the route inventory**: this is the dimension most likely to produce a
  P0. Confirm every canonical and legacy URL still resolves, in all three languages, and that
  the sitemap does not advertise a URL the deploy drops.
- **Tests**: confirm the leak test asserts over the built tree rather than over the allowlist,
  and that it fails when it should — an auditor should plant a file and watch it fail.
```
