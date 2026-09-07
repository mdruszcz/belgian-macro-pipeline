# Reference designs — how these were captured, and one open question

## Status

Four reference designs were shared in chat on 2026-09-07: the microeconomics page, the
macroeconomics page, a municipality profile (Namur), and the homepage.

**They arrived as images pasted into the conversation, not as files.** Nothing was saved to disk
by the pasting itself — no PNG, no Figma link, no export. What's in this directory is a careful
written transcription, made while looking directly at them: layout structure, every visible
label and figure, approximate colours, and component inventory.

That's enough to build from — Batch 1 (design tokens) and Batches 2-4/6/7 (components and page
reproduction) proceed against these transcriptions. It is **not** the same as having the source
files. A transcription can get a colour approximately right from a screenshot; it cannot get it
exactly right the way a colour picker on the real asset can, and it can't be handed to
`visual-auditor` for a pixel-diff — there's nothing to diff against.

**Ask, not a blocker:** if the original files exist (Figma, Sketch, exported PNGs at full
resolution, anything), dropping them into `docs/design-references/originals/` would let a later
pass tighten the token values from "read off a screenshot" to "measured." Not needed to keep
moving — just better when available.

## The one thing that needs a decision, not a guess

**Two different logo treatments and two different brand-name spacings appear across the four
designs**, and this is a real conflict in the source material, not something to silently pick a
side on (the same rule that says don't guess a data schema applies to a visual one):

- **Macro and micro pages**: wordmark **"Belpulse.be"** (lowercase p, `.be` suffix styled as part
  of the mark), with a small ascending-bars pictogram (three bars, roughly black/yellow/red).
  Tagline underneath: *"Comprendre aujourd'hui. Construire demain."*
- **Homepage and the Namur profile**: wordmark **"BelPulse"** (capital P, no `.be`), with a
  vertical-striped-bars pictogram (also reading as the Belgian tricolour, but a different shape).
  Tagline: *"Les données qui font avancer la Belgique."*

These might be two drafts of the same brand, an intentional variation between the "analytical
tool" pages and the "public/marketing" pages, or a mistake in how the mockups were assembled.
**Recorded here rather than resolved** — see `docs/implementation/known-risks.md`. Batch 1 below
proceeds with a single token set and a placeholder logo mark either way; the wordmark text and
final pictogram are a one-line change once this is answered, not a blocker to building the token
system underneath them.

## The structural finding that matters most

**These four pages are not one page shell — they are two.**

| Shell | Pages | Navigation | Theme |
|---|---|---|---|
| **Analytical** | Macro, Micro | Top bar + **left sidebar** with icon nav | Micro = light; Macro = dark navy |
| **Editorial** | Homepage, Municipality profile | Top bar only, no sidebar, breadcrumb, hero imagery, card sections, dark CTA band before the footer | Light (both) |

This matches the plan almost exactly — `docs/features/page_builder.md`'s Batch 6 spec already
says "analytical sidebar" for macro, and Batch 7 says micro "reuses the macro analytical shell."
What's newly confirmed is that **macro and micro are the same shell in two themes** (light
institutional / navy analytical — literally what Batch 1's spec already named, before these
designs existed to confirm it), and that the homepage and the municipality profile are a
*third*, distinct shell that Batch 4's spec didn't originally call out as shared with the
homepage. Worth building the editorial shell once, for both.

## Files in this directory

- `micro.md`, `macro.md`, `municipality-profile.md`, `homepage.md` — one transcription per
  design, in the order above
- `component-inventory.md` — every distinct UI component seen across all four, de-duplicated,
  which is what Batch 2 (shared presentation components) builds against
- `tokens-measured.md` — the colour, type, spacing and breakpoint values Batch 1 actually used,
  with a note on which are read directly off the screenshots and which are inferred/estimated
