# Feature: BelPulse design tokens and shared components

Status: Batch 1 (tokens) done 2026-09-07; Batch 2 (shared components) not started
Issue: none (part of docs/features/page_builder.md, Batches 1-2)
Branch: not started

## Problem

The redesign work (Batches 1-8) needs one set of design tokens (colour, type, spacing, breakpoints)
measured from the maintainer's supplied designs, so no later page invents its own. That
measurement cannot happen without the actual design files.

## Goal

Once designs are supplied: a token set (`assets/belpulse/tokens.css`), a component gallery
demonstrating every shared block in every data state, and a light-institutional and a
navy-analytical surface, both driven by the same tokens.

## Non-goals

- Removing the current CSS custom-property system (`--bg`, `--surface`, `--accent`, etc.) used by
  `communes.html`, `map.html`, `local.html` and the shared map component. The new token set sits
  alongside it until pages are actually converted (Batch 15); nothing is deleted early.
- Guessing colours, spacing or type scale from a verbal description. This is the same rule that
  governs data (claude.md rule 13) applied to a design brief instead of a source schema.

## Proposed approach

Measure, don't invent: colours, typography, navigation height, sidebar width, card dimensions,
borders, shadows, spacing, chart density and responsive behaviour, taken directly from the
supplied files. Define semantic tokens (`--surface-card`, not `--grey-2`) so a later retheme is a
token edit, not a find-and-replace across every block.

Breakpoints: desktop / tablet / mobile, values to be fixed once the designs are measured.

## Tests

- Token existence and naming (a lint step, not a visual one)
- Colour-contrast checks against WCAG AA
- Font loading and fallback behaviour
- Component-gallery screenshots at every required viewport
- 200% zoom, long French and Dutch labels (both known failure modes for a design system built
  against English-only mockups)
- No indicator IDs anywhere in a design asset (claude.md rule 24)

## What actually happened (2026-09-07)

The four reference designs arrived as **images pasted into chat, not files**. Full transcription
of all four, plus a de-duplicated component inventory and the measured/estimated token values,
is in `docs/design-references/` — that directory is now the authoritative record, not this
document's earlier speculative section.

Two real findings from looking closely at the designs, not implied by the plan beforehand:

1. **Two page shells, not one.** Macro/micro share an "analytical" shell (top bar + left
   sidebar), in a light variant (micro) and a dark "navy-analytical" variant (macro) — literally
   the two surfaces this document already named before the designs existed to confirm it.
   Homepage/municipality-profile share a completely different "editorial" shell (top bar only,
   breadcrumb, hero imagery, dark CTA band). Batch 2 builds both.
2. **Two logo/brand-name treatments appear across the four designs** ("Belpulse.be" vs
   "BelPulse", two different pictograms) — a genuine conflict in the source material, not
   resolved here. See `docs/design-references/README.md`.

`assets/belpulse/tokens.css` is built and tested (`tests/pages/test_design_tokens.py`, 17
tests): colour, chart palette, type scale, spacing, radius, shadow, both themes. Every colour
that started below WCAG contrast was caught by actually computing the ratio (not eyeballing a
screenshot) and darkened with margin, not to the bare minimum — see the token file's own
comments for the before/after numbers. A working component gallery
(`docs/design-references/token-gallery.html`) renders every token live, in both themes, verified
in a real headless browser with zero console errors.

## Assumptions and open questions

- **No breakpoints were informed by the designs at all** — all four supplied images are
  desktop-only. Tablet/mobile follow this repo's own existing convention until real mobile
  mockups exist.
- Typeface is a **recommendation** (Inter for UI, Caveat for the decorative pull-quote), not a
  confirmed match — no typeface can be identified with confidence from a screenshot.
- Whether the new tokens eventually replace the current CSS custom-property system, or the two
  are reconciled into one, is a decision for Batch 15 (page conversion), not this batch.
- The two open items from `docs/design-references/README.md` (logo/brand-name conflict, no
  original design files in the repo) carry forward unresolved.

## Rollout / risks

Every visual batch (1, 2, 3, 4, 6, 7) depends on this one being right. Getting the token set
wrong is the kind of mistake that surfaces sixty pages later as a hundred small inconsistencies,
which is exactly why it is measured against real files rather than approximated.
