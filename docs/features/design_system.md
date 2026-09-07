# Feature: BelPulse design tokens and shared components

Status: blocked — needs the supplied reference designs
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

## Assumptions and open questions

- **Blocked entirely on the maintainer supplying the reference designs** (homepage, macro, micro,
  municipality/Namur). Nothing in this document can be finalised before then.
- Whether the new tokens eventually replace the current CSS custom-property system, or the two
  are reconciled into one, is a decision for Batch 15 (page conversion), not this batch.

## Rollout / risks

Every visual batch (1, 2, 3, 4, 6, 7) depends on this one being right. Getting the token set
wrong is the kind of mistake that surfaces sixty pages later as a hundred small inconsistencies,
which is exactly why it is measured against real files rather than approximated.
