# Reference: Municipality profile (Namur)

Editorial shell — top nav only, no left sidebar. This is the design Batch 4
(`docs/steps`) reproduces as a generic template, tested against Namur, Antwerp, Herstappe, a
Brussels commune, a 2025-merger successor, and a suppressed-value commune — never hardcoded to
Namur.

## Shell

- **Top bar** (white): "BelPulse" wordmark (capital P, no `.be`) + a vertical-striped pictogram,
  tagline "Les données qui font avancer la Belgique." Nav: Accueil, Profils communaux (active,
  red underline), Macro, Micro, Cartes, Méthodologie. Search box ("Rechercher une commune, une
  province, un indicateur…"), language dropdown.
- **Breadcrumb**: Accueil > Profils communaux > Namur.

## Page header

- H1 "Profil communal — Namur", subtitle "Région wallonne · Province de Namur".
- Three small badges: a pin icon + "Commune", a clock icon + "Dernière mise à jour : aujourd'hui",
  a checkmark + "Sources officielles".
- Right-aligned action buttons: "⊞ Comparer", "⤴ Partager", "⬇ Télécharger le profil" (filled,
  dark). The download button is real functionality this pipeline already has — `/local` already
  links to the commune's own JSON payload; this is that same feature, restyled.

## Hero

Two columns: a large photo of the commune (Namur's citadel/riverfront) with the commune name and
a one-line tagline overlaid, plus a small caption pin ("📍 Citadelle de Namur" — commune-specific
editorial copy, not data-driven, and out of scope for the binding layer entirely: **this
pipeline has no photo library**, and nothing about a commune's chosen photo or tagline can come
from a payload — see "Open question" below). Beside it, a small locator map of Belgium with the
commune highlighted within its region, zoom controls, "Voir sur la carte →".

Beside the hero: an intro paragraph (generic boilerplate, not commune-specific — this one **is**
templatable) and a 2×3 grid of mini stat tiles: Population, Revenu médian, Taux de chômage,
Entreprises, Solde migratoire, Âge médian — each with a value and a "vs. 2023" delta.

## Section tabs

Vue d'ensemble (active), Population, Économie, Emploi, Finances locales, Logement, Mobilité,
Environnement — this maps closely onto the existing `/local` page's own section list
(`config/local_sections.yaml`) already in production, just restyled as tabs instead of stacked
sections. Worth checking whether the redesign keeps them as tabs (single view, switched) or
flattens them back to a scrolling page like today — a real layout decision, not implied by the
mockup alone (a static screenshot can't show which).

## Body content

1. **"Chiffres clés et tendances récentes"** — six small charted stat cards (Population, Revenu
   médian, Taux de chômage, Entreprises actives, Prix des logements, Recettes fiscales locales),
   each with a small trend chart, 2015–2024.
2. **Three-column row**: "Évolution des principaux indicateurs" (indexed to 100 in 2015,
   multi-line — this is exactly the `index_base_100` derived function this pipeline already
   has); "Structure de l'emploi" (horizontal % breakdown); "Répartition par âge" (donut chart) —
   this pipeline already stores age bands (`POPULATION_AGE_0_14/15_64/65_PLUS`) at three bands,
   not the four shown (0–17, 18–34, 35–64, 65+) — **a real mismatch**, not a rendering detail:
   either the mockup's bands need remapping to what's actually stored, or a finer age
   breakdown needs a new source, which is exactly the kind of thing that needs a
   `docs/data_catalog.md` row (rule 8) before it's built, not silently reinterpreted.
3. **"Finances locales"** — 4 stat cards (Budget, Dépenses, Recettes, Dette par habitant) plus a
   debt-per-capita line chart. **This pipeline has no municipal finance data yet** — that's
   Block O (Walloon municipal finance) in Phase II, not built. This whole section is blocked on
   that block landing, not on the redesign itself.
4. **Two maps**: population by statistical sector (finer than commune level — this pipeline's
   boundary file is commune-level only, from `docs/data_catalog.md`'s Block X notes; a
   sector-level choropleth needs sector-level geometry this repo doesn't have), and a
   neighbouring-communes locator map.
5. **"Namur comparée"** — a comparison table (indicator / this commune / regional average /
   national average / both deltas) — maps directly onto the existing `comparison` payload shape.
6. **"Focus thématiques"** — six small link-out cards (Démographie, Économie locale, Marché du
   travail, Logement, Finances, Cadre de vie), each a one-line editorial blurb. Static content,
   not data-bound.

## Right sidebar (this shell's sidebar is on the RIGHT, unlike the analytical shell's left one)

- "Comparer [commune]" — a peer-picker panel. This pipeline already has this exact feature live
  on `/local` (the free comparison picker, up to 3 peers, `?vs=` in the URL) — restyle, don't
  rebuild.
- "Classement de [commune]" — a ranking list (5 named rankings, each "N / 581"). This pipeline
  has percentiles per indicator already; a *named, composite* ranking like "Dynamisme économique"
  or "Attractivité résidentielle" does not exist yet and isn't a single indicator — it reads like
  Peer Model / Fiscal Score territory (Blocks M/P, not yet built). Flagged, not silently faked
  with an invented formula.
- "Communes voisines" — a small list of neighbouring communes with population and a delta.
- "Sources et méthodologie" — text block + link.
- "Liens utiles" — three utility links (map, CSV export, methodology).

## Bottom CTA band + footer

Full-width dark navy band: "Explorez les 581 communes belges" + two buttons + three stats (581
communes, 200+ indicateurs, mises à jour automatiques). **"581 communes" and "200+ indicateurs"
are both aspirational, not current** — this pipeline has 565 communes (post-2025 merger count,
already the pipeline's own verified figure) and 52 municipal indicators today. The mockup's
copy needs those numbers to either come from the real manifest (`public/data/manifest.json`
already carries live counts) or be written honestly as a target, never hardcoded to a number
already known to be wrong (claude.md rule 36).

Footer: dark, logo + tagline, nav links, social icons, language selector, a Belgian-flag graphic
with the line "Une donnée partagée pour une Belgique plus forte."

## Open questions

- The hero photo and tagline are per-commune editorial content this pipeline has no source for
  (no photo library, no commune taglines anywhere in the data model). Needs a decision: skip the
  photo entirely for communes without one (honest, matches this pipeline's existing "no data, no
  fabricated content" discipline), source stock photography per commune (a real content project,
  not a data one), or use a generic illustration keyed only by region/province.
- Section tabs vs. today's scrolling `/local` layout — a UX decision, not implied by a static
  image.
- The right-sidebar "named rankings" (Dynamisme économique, etc.) need either a defined formula
  (Peer Model / Fiscal Score, not built yet) or they're deferred until those blocks exist.
