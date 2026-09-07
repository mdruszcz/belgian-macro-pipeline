# Reference: Homepage

Editorial shell, same as `municipality-profile.md` — same top bar (nav: Accueil active, Profils
communaux, Macro, Micro, Cartes, Méthodologie), same dark CTA band before the footer, same
footer.

## Hero, two columns

- **Left**: eyebrow label "DONNÉES · TERRITOIRES · PROSPÉRITÉ" (small caps, letter-spaced), H1
  "Les données économiques de la Belgique, mises à jour automatiquement.", a subtitle paragraph,
  two buttons ("Explorer les données", filled red; "▶ Voir la vidéo", outline dark).
- **Right**: a large national choropleth (PIB par habitant by commune, 2023) with a hover
  tooltip example shown ("Bruxelles 56 430 €"), zoom controls, a 5-bucket legend, an indicator
  dropdown. Below it, two small chart cards: "PIB réel de la Belgique (base 2015 = 100)" and
  "Taux d'inflation" — both real, existing national series.
- Small decorative handwritten-style tag, bottom-right of the hero: "Une Belgique plus éclairée"
  + a Belgian flag graphic. Purely decorative, matches the same treatment as the small quotes on
  the analytical pages — a recurring brand device across all four designs, worth keeping as one
  reusable "editorial pull-quote" component rather than four one-off implementations.

## "Finances publiques" band

Four inline stat tiles: Dépenses de l'État, Recettes de l'État, Solde budgétaire (red, negative),
Dette publique — "Voir tous les indicateurs →". **Same open question as the macro page's
"comptes simulés en temps réel"**: is this the same simulated-counter component restyled as
static tiles, or genuinely static current-figure tiles? The homepage version shows plain
numbers, not the ticking-counter treatment — likely the honest static form of the same data,
reusing the *values* but not the *simulation* component. Worth confirming rather than assuming
either way when this is built.

## "Profils des communes"

A short blurb, then one commune preview card (Namur, in this mockup) — photo, a stat grid, "Voir
le profil complet →" — and "Explorer toutes les communes →". This is a single hardcoded example
in the mockup; the real block needs to either always show a fixed example commune (configured,
not hardcoded in a template — a page-document `props` field, not a magic string in renderer
code) or rotate/feature one editorially. A decision, not a default to assume.

## "Belgique en cartes"

A tab selector (Économie active, Population, Emploi, Fiscalité, Logement, Environnement) above
three small map thumbnails, each linking out to a fuller map — "Voir toutes les cartes →". This
maps directly onto the existing `map.html`/indicator-picker pattern already built and shared
(`assets/commune_map.js`) — this section is "the shared map component, three times, small," not
new map logic.

## "Indicateurs clés de l'économie belge"

Six mini chart cards (Croissance du PIB, Inflation, Emploi, Productivité, Démographie, Taux de
pression fiscale) — "Voir tous les indicateurs macroéconomiques →". Same KPI-card pattern as the
macro/micro pages, at a smaller size — one component, several size variants, not a new one.

## Four-icon feature band

"Mise à jour automatique", "Sources officielles", "Traçabilité et transparence", "Au service des
citoyens et des décideurs" — icon + title + one-line description each. Purely editorial/marketing
copy, no data binding.

## Bottom CTA + footer

Same dark band and footer pattern as the municipality profile page — see
`municipality-profile.md` for the "581 communes / 200+ indicateurs" aspirational-numbers note,
which applies identically here.

## What's genuinely new here versus the other three pages

Nothing structurally new — every block on the homepage is a smaller/summary version of a block
that also appears on the micro, macro or municipality-profile pages (KPI cards, choropleth,
comparison table pattern, editorial pull-quote, CTA band, footer). This is good news for Batch 2
(shared components): the homepage doesn't need its own component set, it needs the same
component library at a "preview" density, which is worth designing as a first-class size variant
(`size: "compact"` or similar in the block schema) rather than a copy-pasted second version of
each component.
