# Reference: Microeconomics page

Analytical shell, light theme. French copy shown; every label needs FR/NL/EN per rule 7 once
built as real strings, not hardcoded per-language mockups.

## Shell

- **Top bar** (white): logo top-left — "Belpulse.be" wordmark + small ascending-bars pictogram,
  tagline below it in small grey text ("Comprendre aujourd'hui. Construire demain.", two lines).
  Nav, left to right: Aperçu, Macroéconomie, Microéconomie (active, blue underline), Communes,
  Cartes, Données, À propos. Right-aligned: search input ("Rechercher une commune, un
  indicateur…") with a magnifier icon, then a language dropdown ("FR ⌄").
- **Breadcrumb** below the top bar: "Accueil > Microéconomie".
- **Left sidebar**, dark navy, fixed width, distinct from the light main content:
  - Section title "Microéconomie"
  - Icon + label nav list: Vue d'ensemble (active — filled pill), Entreprises, Ménages, Revenus,
    Marché du travail, Secteurs, Productivité, Immobilier, Prix & consommation, Entrepreneuriat,
    Comparaisons territoriales
  - Below the list, a status card: database icon, "Dernière mise à jour des données", "26 janv.
    2025, 10:42", green dot + "Mise à jour automatique"
  - An italic pull-quote card: *"Des données fiables pour une Belgique plus forte."* with a
    small Belgian-flag colour stripe
  - Sidebar footer: "Belpulse.be" + "Données. Territoires. Avenir."

## Main content, top to bottom

1. **Page header**: H1 "Microéconomie belge", grey subtitle paragraph. To the right, a
   decorative line-art illustration of a Belgian town silhouette plus a small handwritten-style
   quote *"Des données locales pour comprendre l'économie réelle."* and a thin Belgian flag
   stripe. Purely decorative — no data binding.

2. **KPI row**, 6 cards, each: coloured circular icon, label, large figure, small up/down arrow
   with a percentage or point change "vs. 2023", tiny sparkline.
   - Entreprises actives — 1,29 million — ↑ +2,1%
   - Créations d'entreprises — +3,4% — ↑ +3,4%
   - Revenu médian — 26 350 € — ↑ +2,8%
   - Salaire brut moyen — 4 120 € — ↑ +3,1%
   - Taux de chômage — 5,6% — ↓ −0,4 point *(down arrow shown in red — for this one indicator, a
     fall is the good direction, and the card colours the arrow by* preferred_direction*, not by
     a fixed up=green/down=red rule — matches this pipeline's own `preferred_direction` field
     exactly, worth confirming the block honours it the same way)*
   - Prix médian du logement — 285 000 € — ↑ +4,7%

3. **Three-column row**:
   - "Évolution du tissu entrepreneurial" — grouped bar chart, 2015–2024, three series
     (Entreprises actives / Créations / Faillites), a period-range dropdown top-right ("2015 –
     2024")
   - "Indicateurs clés" — a plain list panel, icon + label + value + small unit caption per row:
     Densité d'entreprises (112, "pour 1 000 habitants"), Revenu disponible médian, Part des PME
     (< 250 salariés), Productivité du travail, Taux d'épargne des ménages, Taux d'emploi
     (20–64 ans)
   - "Comparaison territoriale" — horizontal bar chart by province, with the national figure
     (Belgique) visually distinguished (filled in the accent colour where the others are a
     lighter tint), dropdown to pick which indicator is compared

4. **Three-column row**:
   - "Dynamique des ménages" — 4 small stat tiles in a 2×2 grid (icon, label, value, delta)
   - "Carte microéconomique de la Belgique" — choropleth map, dropdown to pick the indicator,
     zoom controls, a 5-bucket legend, "Source : Statbel (2024)" caption
   - "Actualités microéconomiques" — 3 news items, each with a thumbnail image, a date, a
     headline, a one-line excerpt; "Toutes les actualités →" link

5. **Four-column row**:
   - "Structure sectorielle" — horizontal percentage breakdown, one bar per sector
   - "Marché du logement" — line chart, one series, dropdown ("Prix d'achat")
   - "Entrepreneuriat local" — 3 stat rows, each with a trend arrow
   - "Explorer les communes" — a short blurb plus commune-name chips (5 examples) and a
     "Toutes les communes" button

6. **Footer**: "Sources : Statbel, BNB, SPF Finances, Eurostat, BCE, OCDE" (left), "Contact ·
   FAQ · Mentions légales · © 2025 Belpulse.be" (right).

## Notable for the binding layer (docs/features/data_binding.md)

- Every KPI, chart and map has a real value and a real delta "vs. 2023" — this maps directly to
  the existing `comparison`/history payload shape, not a new one.
- The unemployment KPI's arrow colour follows `preferred_direction`, which this pipeline already
  stores per indicator — the block should read that field, never hardcode "down is good."
- "Comparaison territoriale" needs a province-level aggregate with the current geography
  highlighted, which is exactly `comparison.md`'s existing aggregate shape.
- The dropdown-driven chart/map ("pick which indicator") implies a block can be *reconfigurable
  at view time*, not just at build time — worth a note for Batch 10 (renderer): is that client-side
  indicator switching inside one block, or is it out of scope for v1 and every chart is bound to
  one fixed indicator until the builder is used to change it? Flagged as an open question below.
