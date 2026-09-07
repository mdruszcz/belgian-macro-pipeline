# Reference: Macroeconomics page

Same analytical shell as `micro.md`, **dark navy theme** — confirms Batch 1's planned
"navy-analytical surface" is this page's actual look, not a hypothetical second option.

## What differs from the micro page's shell

- Whole page is dark navy, not just the sidebar — top bar, sidebar and main content all share
  one dark background, with cards a slightly lighter navy for contrast.
- Top bar carries a "Mon espace" button, top-right, dark pill with an icon — the first sign of
  an authenticated/account area in any of the four designs. Rule: no user accounts before the
  50% milestone (already in claude.md) — this button exists in the mockup but is out of scope
  for the redesign itself; render it as a static, disabled/decorative element for now, or omit
  it, rather than building anything behind it.
- Text is light-on-dark throughout; chart colours are the same accent palette, just brighter to
  read on a dark background (a real design-token concern: charts likely need light-theme and
  dark-theme colour variants, not one fixed palette — this pipeline's existing dark-mode CSS
  variables for `communes.html`/`map.html` already do exactly this for the choropleth ramp,
  worth reusing that pattern rather than inventing a second one).

## Content, top to bottom

1. **KPI row**, 6 cards: PIB (réel, 2024 T3) +1,2%; Population 11,7 millions; Taux d'emploi
   (20–64 ans) 71,4%; Taux d'inflation 2,8%; Solde budgétaire −4,4% (shown in red — a negative
   balance, correctly coloured as unfavourable); Dette publique 106,2% du PIB.

2. **Three-column row**:
   - "Croissance du PIB réel" — bar chart, 2000–2024, one series, with a visually distinct bar
     for the COVID-era contraction (~2020) — the chart doesn't hide a bad year, it colours it
     differently and lets it read as an outlier, which is worth carrying into the block
     contract's "explicit zero / real extreme" handling
   - "Indicateurs clés" — same list-panel pattern as micro: PIB courant, PIB par habitant,
     Productivité du travail, Taux de chômage (BIT), Taux d'activité, Solde des comptes courants
   - "Comparaison internationale" — horizontal bars, several EU countries plus "Zone euro", with
     "Belgique" bolded/highlighted among them — same "highlight the subject among its peers"
     pattern as micro's provincial comparison, just at the country level

3. **"Finances publiques (compteurs en temps réel, simulés)"** — explicitly labelled
   **"simulés"** (simulated) in the design itself, with an info icon. Large, ticking-style
   counters: Recettes de l'État (with a "+X €/seconde" rate), Dépenses de l'État (same), a
   progress bar under each, and a computed "Solde (simulé)" below both.
   **This is the one component in all four designs that is NOT a real data binding** — it's a
   live-incrementing simulation from an annual/quarterly total, not a per-second measurement
   from any source. Needs its own honest label in the real build (the mockup already has one:
   "simulés" — keep it, make it more prominent if anything, not less) and its own block type
   that's explicit about being an illustrative extrapolation, never presented as a live feed of
   real government spending data. Flagged in `docs/implementation/known-risks.md`.

4. **Two-column row**: "Carte économique de la Belgique" (choropleth, PIB par habitant 2023,
   legend, "Voir la carte interactive →"); "Actualités économiques" (text-only news list, no
   thumbnails here unlike micro's version).

5. **Footer**: "Sources : Statbel, BNB, SPF Finances, Eurostat, OCDE · Données mises à jour
   automatiquement", small logo restated bottom-left.

## Notable for the binding layer

- The "simulated counters" component is real UI, seen in a real design, and needs a real
  decision: build it as an honest, clearly-labelled extrapolation block (recommended — it's
  visually compelling and the mockup already disclaims it correctly), or drop it. Not something
  to build silently either way.
- **International comparison is not buildable today, checked rather than assumed.** The mockup
  shows Belgium against Poland, Spain, the euro area, Germany and the Netherlands.
  `config/indicators/` already has `EUROSTAT_GDP_Q_MEUR_{DE,EA,ES,FR,NL}.yaml` — five of the six
  configs exist — but **none of the five has ever loaded a single observation** (a pre-existing
  DBnomics fetch-timeout issue, already recorded in `docs/steps` under Block H). Poland isn't
  configured at all. This chart is real design work sitting on top of a real, pre-existing data
  gap; Batch 6 needs either that fetch issue fixed first, or the chart scoped down to whichever
  countries genuinely have data by the time it's built. Not a reason to skip the batch — a
  reason to sequence it correctly.
