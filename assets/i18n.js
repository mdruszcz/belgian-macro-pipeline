/* Interface language for the whole site.
 *
 * ONE TABLE, NOT ONE PER PAGE. Before this file, local.html was the only
 * trilingual page and it held the licence attribution three times over, while
 * communes.html and map.html each carried an English fourth copy of the same
 * text -- kept in step only by a test that compared two of them byte for byte.
 * That text is a LICENCE CONDITION on every page publishing municipal data
 * (docs/data_catalog.md), so four copies drifting apart is not a tidiness
 * problem, it is the notice becoming wrong on some pages and not others.
 *
 * Indicator names, section labels, source labels and grade words are NOT here.
 * They travel in the payloads from the indicators table and the configs
 * (CLAUDE.md rules 2 and 7), so a label cannot drift from the data it
 * describes. What lives here is interface chrome: the words the site says
 * about itself.
 *
 * No runtime translation calls, by design -- roadmap Block X: runtime LLM
 * translation is slow, inconsistent between page loads and impossible to
 * proofread. These strings are written once and reviewed once.
 */

const I18N = {};

I18N.LANGS = ['en', 'fr', 'nl'];
I18N.DEFAULT_LANG = 'en';

/* The same localStorage key local.html has always used, so a language chosen
   on one page carries to every other -- exactly as the theme key already does.
   Changing it would silently reset every existing reader's choice. */
I18N.STORAGE_KEY = 'belpulse-lang';

/* THE OTHER KEY. index.html and dashboard.html had their own translation
   tables and their own switcher, persisting to plain `lang`, while local.html
   used `belpulse-lang`. Two language systems on one site, writing to two
   different keys: choosing French on the dashboard and clicking through to the
   commune table gave you English. Reproduced in a browser before fixing.
   Those pages now write the canonical key, and this is read as a fallback so
   nobody who had already chosen a language loses it. */
I18N.LEGACY_STORAGE_KEY = 'lang';

I18N.STRINGS = {
  en: {
    language: 'Language',
    theme: 'Colour theme',
    themeLight: 'Light',
    themeAuto: 'Auto',
    themeDark: 'Dark',
    loading: 'Loading…',
    communesTitle: 'Commune data',
    communesLead: 'Every municipal-level observation this pipeline holds — every year on record, one row per commune. Pick a year below; hover a value for its full history. The full underlying rows are in <code>data/communes_history.csv</code>, for a researcher who wants everything at once. Or press <strong>Show map</strong> to draw the column you are sorting by, in the year you have picked. See <a href="map.html">map.html</a> for a full-page map of the latest figures, <a href="all_data.html">all_data.html</a> for national-level data, or <a href="local.html">local.html</a> for a single commune’s own profile page.',
    statCommunes: 'Communes',
    statIndicators: 'Indicators',
    statYears: 'Years covered',
    statUpdated: 'Last fetched',
    filterPlaceholder: 'Filter by commune name (EN/FR/NL) or NIS code…',
    allRegions: 'All regions',
    yearHint: 'Not every indicator covers every year — a column shows “n/a” for a year outside its own coverage',
    showMap: 'Show map',
    hideMap: 'Hide map',
    colCommune: 'Commune',
    colRegion: 'Region',
    colProvince: 'Province',
    colArrondissement: 'Arrondissement',
    nCommunes: '{n} communes',
    nOfCommunes: '{n} of {total} communes',
    communesFooter: 'Read from GitHub Pages — no server, no filtering applied. Indicator columns appear on their own as new municipal datasets are added. Hover any value for its full year-by-year history.',
    mapTitle: 'Commune map',
    mapLead: 'Every municipal indicator this pipeline holds, drawn on the 565 Belgian communes. Pick an indicator; hover a commune for its value; click it to open its own profile page. Communes are shaded by <em>rank</em>, so each colour band holds roughly the same number of communes. Communes the source has no figure for are drawn flat, never as zero. See <a href="communes.html">communes.html</a> for the same figures as a table.',
    mapFooter: 'Reads the boundary file and the indicator payloads from GitHub Pages — no server, no map tiles, no third-party requests. The indicator list is read from the published metadata, so a new municipal dataset appears in the picker on its own.',
    findCommune: 'Find a commune (name or NIS code)…',
    indicatorToMap: 'Indicator to map',
    zoomIn: 'Zoom in',
    zoomOut: 'Zoom out',
    resetView: 'Reset view',
    communesShown: '{n} of {total} communes shown',
    figuresFor: '{n} of {shown} communes shown have a figure for {y}.',
    noDataInYear: 'No data in {y} — this indicator covers {c}.',
    showYearInstead: 'Show {y} instead',
    retrieved: 'retrieved',
    computedFromShort: 'computed from {s} last updated {d}',
    mapNoData: 'no data',
    mapNoValueHere: 'no data here',
    mapWithheld: 'withheld by the source — fewer than 10, not zero',
    mapNoneCarryValue: 'No commune shown carries a value for this indicator.',
    mapRange: 'Lowest {lo}, highest {hi}.',
    mapColourRuns: 'Colour runs low to high, left to right along the bar.',
    mapMethodQuantile: 'Each band holds roughly the same number of communes, so the colour shows rank rather than distance.',
    mapMethodEqual: 'Too many communes share the same figure for rank-based bands to separate them, so these bands are equal steps in value instead — the colour shows distance from the lowest, not rank.',
    mapBasisFiltered: 'Bands are computed over the communes currently shown, not all 565.',
    mapDirHigher: 'For this indicator the source considers a higher value better.',
    mapDirLower: 'For this indicator the source considers a lower value better.',
    mapDirContextual: 'This indicator has no better or worse direction — it is context, not a score.',
    mapMissingLead: '{n} of the {total} communes shown have no value here and are drawn as “no data”.',
    mapMissingWithheld: '{n} were withheld by the source — a count below 10, suppressed for privacy rather than published, and never a zero.',
    mapMissingUncollected: '{n} have no figure at all: a gap in the source, or a merged commune with no row on the map vintage this outline uses.',
    nis: 'NIS',
    describeLead: 'Municipal statistics for {name}, Belgium',
    describeTail: '{n} indicators with history, sources and comparisons.',
    openInteractive: 'Open the interactive profile — charts, comparisons and peer communes',
    colIndicator: 'Indicator',
    colValue: 'Value',
    colPeriod: 'Period',
    colSource: 'Source',
    colUpdated: 'Updated',
    withheldCell: 'withheld by the source (fewer than 10)',
    withheldSuffix: 'withheld',
    inputsPrefix: 'inputs',
    staticFooter: '{n} indicators shown. Full history for this commune:',
    jsonPayload: 'JSON payload',
    allCommunes: 'all communes',
    allDataTitle: 'All observations',
    allDataLead: 'Every row currently in <code>data/belgian_macro_export.csv</code> — unfiltered, no category grouping. For the presented view see <a href="dashboard.html">dashboard.html</a>.',
    statObservations: 'Observations',
    colCode: 'Code',
    colName: 'Name',
    colStatus: 'Status',
    colUnit: 'Unit',
    colFetched: 'Fetched',
    allDataFooter: 'Read from GitHub Pages · the same files as the main dashboard, no server, no filtering applied.',
    aboutTitle: 'About BelPulse',
    aboutTagline: 'Your window into the Belgian macroeconomic pulse',
    boundariesAttribution: '<strong>Boundaries:</strong> <a href="https://statbel.fgov.be/" rel="noopener">Statbel</a> (Direction générale Statistique — Statistics Belgium), statistical sectors of 1 January 2026, reused under Statbel’s <em>Licence open data</em> of 22 October 2015, the same document already recorded in <code>docs/licences/</code>. <strong>Changes were made:</strong> the 20,781 statistical sectors were merged into the 565 communes, the outlines simplified to a 50&nbsp;m tolerance so the file is small enough to send to a browser, and the coordinates converted from Belgian Lambert 2008 (EPSG:3812) to longitude/latitude. The source is accurate to 1:10,000; these simplified outlines are not, and are drawn for comparison, never for measurement or for locating a boundary on the ground.',
    attribution: '<strong>Source:</strong> <a href="https://statbel.fgov.be/" rel="noopener">Statbel</a> (Direction générale Statistique — Statistics Belgium), reused under <a href="https://creativecommons.org/licenses/by/4.0/" rel="license noopener">CC BY 4.0</a> and Statbel’s <em>Licentie open data</em> of 22 October 2015. <strong>Data last updated:</strong> <span id="attrUpdated">—</span>. A figure computed from these data carries the date its INPUTS were last updated, labelled as theirs and not as its own — it was computed, not retrieved. <strong>Changes were made:</strong> the published files are reshaped into a canonical indicator model, re-keyed to internal geography identifiers, corrected across the 2019 and 2025 commune mergers, and used to compute derived figures. Statbel does not endorse this product or how its data is used here. English commune names are unofficial translations, not official Statbel labels. <br><br><strong>Source:</strong> <a href="https://www.onem.be/" rel="noopener">ONEM/RVA</a> (Office National de l\'Emploi — Rijksdienst voor Arbeidsvoorziening), for the employment section\'s unemployment, temporary-unemployment, part-time-benefit and activation figures. <strong>Date of the information used:</strong> the files fetched for this build, covering 2017 onwards; the current year is a part-year average, marked provisional. Reused under ONEM\'s own conditions, which permit commercial reuse and require the source and this date — <em>not</em> under CC BY 4.0, which does not apply to this source. Figures ONEM withholds for privacy (fewer than 10 people) are shown as suppressed, never as zero. <br><br><strong>Source:</strong> <a href=\"https://www.police.be/statistiques/\" rel=\"noopener\">Police Fédérale / Federale Politie</a> (Direction de l\'information policière et des moyens ICT), for the house-burglary, car-theft, vehicle-theft and domestic-violence rates. Reused under the federal police\'s own condition: correctly credit the source as stated here. That condition says nothing about permitted uses beyond attribution — <em>not</em> under CC BY 4.0, and not a stated grant of commercial reuse either, unlike Statbel and ONEM above. Every one of these rates is resolved onto the commune boundaries in force through 2024, not the current map, for every year shown — including years long before that map existed, since that is how the source itself publishes its own history. The most recent year of each series is marked provisional.',
  },
  fr: {
    language: 'Langue',
    theme: 'Thème',
    themeLight: 'Clair',
    themeAuto: 'Auto',
    themeDark: 'Sombre',
    loading: 'Chargement…',
    communesTitle: 'Données communales',
    communesLead: 'Toutes les observations communales de ce pipeline — chaque année enregistrée, une ligne par commune. Choisissez une année ci-dessous ; survolez une valeur pour voir tout son historique. Les lignes sous-jacentes complètes se trouvent dans <code>data/communes_history.csv</code>, pour qui veut tout d’un coup. Ou cliquez sur <strong>Afficher la carte</strong> pour cartographier la colonne selon laquelle vous triez, pour l’année choisie. Voir <a href="map.html">map.html</a> pour une carte plein écran des derniers chiffres, <a href="all_data.html">all_data.html</a> pour les données nationales, ou <a href="local.html">local.html</a> pour la fiche d’une commune.',
    statCommunes: 'Communes',
    statIndicators: 'Indicateurs',
    statYears: 'Années couvertes',
    statUpdated: 'Dernière extraction',
    filterPlaceholder: 'Filtrer par nom de commune (EN/FR/NL) ou code NIS…',
    allRegions: 'Toutes les régions',
    yearHint: 'Tous les indicateurs ne couvrent pas toutes les années — une colonne affiche « n/a » pour une année hors de sa couverture',
    showMap: 'Afficher la carte',
    hideMap: 'Masquer la carte',
    colCommune: 'Commune',
    colRegion: 'Région',
    colProvince: 'Province',
    colArrondissement: 'Arrondissement',
    nCommunes: '{n} communes',
    nOfCommunes: '{n} communes sur {total}',
    communesFooter: 'Lu depuis GitHub Pages — sans serveur, sans filtrage. Les colonnes d’indicateurs apparaissent d’elles-mêmes à mesure que de nouveaux jeux de données communaux sont ajoutés. Survolez une valeur pour voir son historique année par année.',
    mapTitle: 'Carte des communes',
    mapLead: 'Tous les indicateurs communaux de ce pipeline, représentés sur les 565 communes belges. Choisissez un indicateur ; survolez une commune pour voir sa valeur ; cliquez pour ouvrir sa fiche. Les communes sont colorées par <em>rang</em>, chaque bande contenant donc à peu près le même nombre de communes. Les communes pour lesquelles la source n’a pas de chiffre sont laissées neutres, jamais à zéro. Voir <a href="communes.html">communes.html</a> pour les mêmes chiffres sous forme de tableau.',
    mapFooter: 'Lit le fichier de limites et les données d’indicateurs depuis GitHub Pages — sans serveur, sans tuiles cartographiques, sans requête tierce. La liste des indicateurs provient des métadonnées publiées : un nouveau jeu de données communal apparaît donc tout seul dans le sélecteur.',
    findCommune: 'Rechercher une commune (nom ou code NIS)…',
    indicatorToMap: 'Indicateur à cartographier',
    zoomIn: 'Zoom avant',
    zoomOut: 'Zoom arrière',
    resetView: 'Réinitialiser la vue',
    communesShown: '{n} communes affichées sur {total}',
    figuresFor: '{n} des {shown} communes affichées ont un chiffre pour {y}.',
    noDataInYear: 'Pas de donnée en {y} — cet indicateur couvre {c}.',
    showYearInstead: 'Afficher {y} à la place',
    retrieved: 'extrait le',
    computedFromShort: 'calculé à partir de {s}, mis à jour le {d}',
    mapNoData: 'pas de donnée',
    mapNoValueHere: 'pas de donnée ici',
    mapWithheld: 'non communiqué par la source — moins de 10, et non zéro',
    mapNoneCarryValue: 'Aucune commune affichée n’a de valeur pour cet indicateur.',
    mapRange: 'Minimum {lo}, maximum {hi}.',
    mapColourRuns: 'La couleur va du plus faible au plus élevé, de gauche à droite le long de la barre.',
    mapMethodQuantile: 'Chaque bande contient à peu près le même nombre de communes : la couleur indique donc le rang, non la distance.',
    mapMethodEqual: 'Trop de communes partagent le même chiffre pour que des bandes par rang les séparent ; ces bandes sont donc des pas égaux en valeur — la couleur indique l’écart au minimum, non le rang.',
    mapBasisFiltered: 'Les bandes sont calculées sur les communes actuellement affichées, pas sur les 565.',
    mapDirHigher: 'Pour cet indicateur, la source considère qu’une valeur plus élevée est meilleure.',
    mapDirLower: 'Pour cet indicateur, la source considère qu’une valeur plus faible est meilleure.',
    mapDirContextual: 'Cet indicateur n’a pas de sens meilleur ou pire — c’est un contexte, pas une note.',
    mapMissingLead: '{n} des {total} communes affichées n’ont pas de valeur ici et sont dessinées en « pas de donnée ».',
    mapMissingWithheld: '{n} ont été masquées par la source — un effectif inférieur à 10, supprimé pour raison de confidentialité plutôt que publié, et jamais un zéro.',
    mapMissingUncollected: '{n} n’ont aucun chiffre : une lacune de la source, ou une commune fusionnée sans ligne pour la version de carte utilisée ici.',
    nis: 'NIS',
    describeLead: 'Statistiques communales pour {name}, Belgique',
    describeTail: '{n} indicateurs avec historique, sources et comparaisons.',
    openInteractive: 'Ouvrir la fiche interactive — graphiques, comparaisons et communes semblables',
    colIndicator: 'Indicateur',
    colValue: 'Valeur',
    colPeriod: 'Période',
    colSource: 'Source',
    colUpdated: 'Mise à jour',
    withheldCell: 'non communiqué par la source (moins de 10)',
    withheldSuffix: 'non communiqué',
    inputsPrefix: 'données sources',
    staticFooter: '{n} indicateurs affichés. Historique complet de cette commune :',
    jsonPayload: 'données JSON',
    allCommunes: 'toutes les communes',
    allDataTitle: 'Toutes les observations',
    allDataLead: 'Toutes les lignes actuellement dans <code>data/belgian_macro_export.csv</code> — sans filtre ni regroupement par catégorie. Pour la vue présentée, voir <a href="dashboard.html">dashboard.html</a>.',
    statObservations: 'Observations',
    colCode: 'Code',
    colName: 'Nom',
    colStatus: 'Statut',
    colUnit: 'Unité',
    colFetched: 'Extrait le',
    allDataFooter: 'Lu depuis GitHub Pages · les mêmes fichiers que le tableau de bord principal, sans serveur, sans filtrage.',
    aboutTitle: 'À propos de BelPulse',
    aboutTagline: 'Votre fenêtre sur le pouls macroéconomique belge',
    boundariesAttribution: '<strong>Limites :</strong> <a href="https://statbel.fgov.be/" rel="noopener">Statbel</a> (Direction générale Statistique — Statistics Belgium), secteurs statistiques au 1er janvier 2026, réutilisés sous la <em>Licence open data</em> de Statbel du 22 octobre 2015, le document déjà consigné dans <code>docs/licences/</code>. <strong>Des modifications ont été apportées :</strong> les 20 781 secteurs statistiques ont été fusionnés en 565 communes, les contours simplifiés à une tolérance de 50&nbsp;m afin que le fichier reste assez léger pour un navigateur, et les coordonnées converties du Lambert belge 2008 (EPSG:3812) en longitude/latitude. La source est précise au 1:10 000 ; ces contours simplifiés ne le sont pas et servent à la comparaison, jamais à la mesure ni à la localisation d’une limite sur le terrain.',
    attribution: '<strong>Source :</strong> <a href="https://statbel.fgov.be/" rel="noopener">Statbel</a> (Direction générale Statistique — Statistics Belgium), réutilisé sous <a href="https://creativecommons.org/licenses/by/4.0/" rel="license noopener">CC BY 4.0</a> et sous la <em>Licentie open data</em> de Statbel du 22 octobre 2015. <strong>Dernière mise à jour des données :</strong> <span id="attrUpdated">—</span>. Un chiffre calculé à partir de ces données porte la date de dernière mise à jour de ses DONNÉES SOURCES, présentée comme la leur et non comme la sienne — il a été calculé, non extrait. <strong>Des modifications ont été apportées :</strong> les fichiers publiés sont restructurés selon un modèle d’indicateurs canonique, ré-indexés sur des identifiants géographiques internes, corrigés pour les fusions de communes de 2019 et 2025, et utilisés pour calculer des indicateurs dérivés. Statbel n’approuve ni ce produit ni l’usage qui est fait de ses données. Les noms de communes en anglais sont des traductions non officielles, et non des libellés officiels de Statbel. <br><br><strong>Source :</strong> <a href="https://www.onem.be/" rel="noopener">ONEM/RVA</a> (Office National de l\'Emploi — Rijksdienst voor Arbeidsvoorziening), pour les chiffres du chômage, du chômage temporaire, du temps partiel et de l\'activation dans la section Emploi. <strong>Date des informations utilisées :</strong> les fichiers récupérés pour cette version, couvrant 2017 et suivantes ; l\'année en cours est une moyenne partielle, signalée comme provisoire. Réutilisé selon les conditions propres à l\'ONEM, qui autorisent la réutilisation commerciale et exigent la source et cette date — et <em>non</em> sous CC BY 4.0, qui ne s\'applique pas à cette source. Les chiffres que l\'ONEM masque pour préserver la vie privée (moins de 10 personnes) sont indiqués comme supprimés, jamais comme zéro. <br><br><strong>Source :</strong> <a href=\"https://www.police.be/statistiques/\" rel=\"noopener\">Police Fédérale / Federale Politie</a> (Direction de l\'information policière et des moyens ICT), pour les taux de cambriolage dans habitation, de vol de voiture, de vol de véhicule et de violence intrafamiliale. Réutilisé selon la condition propre à la police fédérale : mentionner correctement la source telle qu\'indiquée ici. Cette condition ne dit rien des usages permis au-delà de l\'attribution — <em>non</em> sous CC BY 4.0, et sans octroi énoncé de réutilisation commerciale, contrairement à Statbel et à l\'ONEM ci-dessus. Chacun de ces taux est calculé sur les limites communales en vigueur jusqu\'en 2024, pas la carte actuelle, pour chaque année affichée — y compris des années bien antérieures à cette carte, car c\'est ainsi que la source publie elle-même son historique. L\'année la plus récente de chaque série est indiquée comme provisoire.',
  },
  nl: {
    language: 'Taal',
    theme: 'Thema',
    themeLight: 'Licht',
    themeAuto: 'Auto',
    themeDark: 'Donker',
    loading: 'Laden…',
    communesTitle: 'Gemeentegegevens',
    communesLead: 'Alle gemeentelijke observaties in deze pipeline — elk vastgelegd jaar, één rij per gemeente. Kies hieronder een jaar; beweeg over een waarde voor de volledige geschiedenis. De volledige onderliggende rijen staan in <code>data/communes_history.csv</code>, voor wie alles in één keer wil. Of klik op <strong>Kaart weergeven</strong> om de kolom waarop u sorteert te karteren, voor het gekozen jaar. Zie <a href="map.html">map.html</a> voor een kaart op volledige pagina van de meest recente cijfers, <a href="all_data.html">all_data.html</a> voor nationale gegevens, of <a href="local.html">local.html</a> voor de fiche van één gemeente.',
    statCommunes: 'Gemeenten',
    statIndicators: 'Indicatoren',
    statYears: 'Bestreken jaren',
    statUpdated: 'Laatst opgehaald',
    filterPlaceholder: 'Filteren op gemeentenaam (EN/FR/NL) of NIS-code…',
    allRegions: 'Alle gewesten',
    yearHint: 'Niet elke indicator bestrijkt elk jaar — een kolom toont “n/a” voor een jaar buiten haar bereik',
    showMap: 'Kaart weergeven',
    hideMap: 'Kaart verbergen',
    colCommune: 'Gemeente',
    colRegion: 'Gewest',
    colProvince: 'Provincie',
    colArrondissement: 'Arrondissement',
    nCommunes: '{n} gemeenten',
    nOfCommunes: '{n} van {total} gemeenten',
    communesFooter: 'Gelezen vanaf GitHub Pages — geen server, geen filtering. Indicatorkolommen verschijnen automatisch zodra nieuwe gemeentelijke datasets worden toegevoegd. Beweeg over een waarde voor de geschiedenis per jaar.',
    mapTitle: 'Gemeentekaart',
    mapLead: 'Alle gemeentelijke indicatoren in deze pipeline, getekend op de 565 Belgische gemeenten. Kies een indicator; beweeg over een gemeente voor de waarde; klik om haar fiche te openen. Gemeenten zijn gekleurd op <em>rang</em>, zodat elke kleurband ongeveer evenveel gemeenten bevat. Gemeenten waarvoor de bron geen cijfer heeft, blijven neutraal — nooit nul. Zie <a href="communes.html">communes.html</a> voor dezelfde cijfers als tabel.',
    mapFooter: 'Leest het grenzenbestand en de indicatorgegevens vanaf GitHub Pages — geen server, geen kaarttegels, geen verzoeken aan derden. De indicatorlijst komt uit de gepubliceerde metadata, zodat een nieuwe gemeentelijke dataset automatisch in de keuzelijst verschijnt.',
    findCommune: 'Zoek een gemeente (naam of NIS-code)…',
    indicatorToMap: 'Indicator om te karteren',
    zoomIn: 'Inzoomen',
    zoomOut: 'Uitzoomen',
    resetView: 'Weergave herstellen',
    communesShown: '{n} van {total} gemeenten weergegeven',
    figuresFor: '{n} van de {shown} weergegeven gemeenten hebben een cijfer voor {y}.',
    noDataInYear: 'Geen gegevens in {y} — deze indicator bestrijkt {c}.',
    showYearInstead: 'Toon {y} in plaats daarvan',
    retrieved: 'opgehaald op',
    computedFromShort: 'berekend uit {s}, laatst bijgewerkt op {d}',
    mapNoData: 'geen gegevens',
    mapNoValueHere: 'hier geen gegevens',
    mapWithheld: 'niet vrijgegeven door de bron — minder dan 10, en niet nul',
    mapNoneCarryValue: 'Geen enkele weergegeven gemeente heeft een waarde voor deze indicator.',
    mapRange: 'Laagste {lo}, hoogste {hi}.',
    mapColourRuns: 'De kleur loopt van laag naar hoog, van links naar rechts langs de balk.',
    mapMethodQuantile: 'Elke band bevat ongeveer evenveel gemeenten, dus de kleur toont rang en niet afstand.',
    mapMethodEqual: 'Te veel gemeenten delen hetzelfde cijfer om ze met rangbanden te scheiden; deze banden zijn daarom gelijke stappen in waarde — de kleur toont de afstand tot het laagste, niet de rang.',
    mapBasisFiltered: 'De banden worden berekend over de nu weergegeven gemeenten, niet over alle 565.',
    mapDirHigher: 'Voor deze indicator beschouwt de bron een hogere waarde als beter.',
    mapDirLower: 'Voor deze indicator beschouwt de bron een lagere waarde als beter.',
    mapDirContextual: 'Deze indicator heeft geen betere of slechtere richting — het is context, geen score.',
    mapMissingLead: '{n} van de {total} weergegeven gemeenten hebben hier geen waarde en worden als “geen gegevens” getekend.',
    mapMissingWithheld: '{n} zijn door de bron achtergehouden — een aantal onder 10, om privacyredenen onderdrukt in plaats van gepubliceerd, en nooit een nul.',
    mapMissingUncollected: '{n} hebben helemaal geen cijfer: een hiaat bij de bron, of een gefuseerde gemeente zonder rij op de kaartversie die deze omtrek gebruikt.',
    nis: 'NIS',
    describeLead: 'Gemeentelijke statistieken voor {name}, België',
    describeTail: '{n} indicatoren met geschiedenis, bronnen en vergelijkingen.',
    openInteractive: 'Open de interactieve fiche — grafieken, vergelijkingen en vergelijkbare gemeenten',
    colIndicator: 'Indicator',
    colValue: 'Waarde',
    colPeriod: 'Periode',
    colSource: 'Bron',
    colUpdated: 'Bijgewerkt',
    withheldCell: 'niet vrijgegeven door de bron (minder dan 10)',
    withheldSuffix: 'niet vrijgegeven',
    inputsPrefix: 'broncijfers',
    staticFooter: '{n} indicatoren weergegeven. Volledige geschiedenis van deze gemeente:',
    jsonPayload: 'JSON-gegevens',
    allCommunes: 'alle gemeenten',
    allDataTitle: 'Alle observaties',
    allDataLead: 'Alle rijen die momenteel in <code>data/belgian_macro_export.csv</code> staan — ongefilterd, zonder groepering per categorie. Voor de gepresenteerde weergave, zie <a href="dashboard.html">dashboard.html</a>.',
    statObservations: 'Observaties',
    colCode: 'Code',
    colName: 'Naam',
    colStatus: 'Status',
    colUnit: 'Eenheid',
    colFetched: 'Opgehaald',
    allDataFooter: 'Gelezen vanaf GitHub Pages · dezelfde bestanden als het hoofddashboard, geen server, geen filtering.',
    aboutTitle: 'Over BelPulse',
    aboutTagline: 'Uw venster op de Belgische macro-economische puls',
    boundariesAttribution: '<strong>Grenzen:</strong> <a href="https://statbel.fgov.be/" rel="noopener">Statbel</a> (Algemene Directie Statistiek — Statistics Belgium), statistische sectoren van 1 januari 2026, hergebruikt onder Statbels <em>Licentie open data</em> van 22 oktober 2015, hetzelfde document dat al is vastgelegd in <code>docs/licences/</code>. <strong>Er werden wijzigingen aangebracht:</strong> de 20.781 statistische sectoren zijn samengevoegd tot de 565 gemeenten, de contouren vereenvoudigd tot een tolerantie van 50&nbsp;m zodat het bestand klein genoeg is voor een browser, en de coördinaten omgezet van Belgische Lambert 2008 (EPSG:3812) naar lengte- en breedtegraad. De bron is nauwkeurig tot 1:10.000; deze vereenvoudigde contouren zijn dat niet en dienen ter vergelijking, nooit voor meting of om een grens op het terrein te bepalen.',
    attribution: '<strong>Bron:</strong> <a href="https://statbel.fgov.be/" rel="noopener">Statbel</a> (Algemene Directie Statistiek — Statistics Belgium), hergebruikt onder <a href="https://creativecommons.org/licenses/by/4.0/" rel="license noopener">CC BY 4.0</a> en Statbels <em>Licentie open data</em> van 22 oktober 2015. <strong>Gegevens laatst bijgewerkt:</strong> <span id="attrUpdated">—</span>. Een berekend cijfer vermeldt de datum waarop de BRONCIJFERS voor het laatst zijn bijgewerkt, aangeduid als de hunne en niet als de zijne — het is berekend, niet opgehaald. <strong>Er werden wijzigingen aangebracht:</strong> de gepubliceerde bestanden worden omgezet naar een canoniek indicatormodel, opnieuw gekoppeld aan interne geografische identificatoren, gecorrigeerd voor de gemeentefusies van 2019 en 2025, en gebruikt om afgeleide cijfers te berekenen. Statbel onderschrijft dit product of het gebruik van zijn gegevens niet. Engelse gemeentenamen zijn onofficiële vertalingen, geen officiële Statbel-labels. <br><br><strong>Bron:</strong> <a href="https://www.onem.be/" rel="noopener">RVA/ONEM</a> (Rijksdienst voor Arbeidsvoorziening — Office National de l\'Emploi), voor de cijfers over werkloosheid, tijdelijke werkloosheid, deeltijdse uitkeringen en activering in de sectie Werk. <strong>Datum van de gebruikte informatie:</strong> de bestanden die voor deze build zijn opgehaald, met cijfers vanaf 2017; het huidige jaar is een gedeeltelijk jaargemiddelde en wordt als voorlopig aangeduid. Hergebruikt onder de eigen voorwaarden van de RVA, die commercieel hergebruik toestaan en de bron en deze datum vereisen — <em>niet</em> onder CC BY 4.0, dat niet op deze bron van toepassing is. Cijfers die de RVA om privacyredenen achterhoudt (minder dan 10 personen) worden als onderdrukt weergegeven, nooit als nul. <br><br><strong>Bron:</strong> <a href=\"https://www.police.be/statistiques/\" rel=\"noopener\">Federale Politie / Police Fédérale</a> (Directie van de politionele informatie en ICT-middelen), voor de cijfers over woninginbraak, autodiefstal, voertuigdiefstal en intrafamiliaal geweld. Hergebruikt onder de eigen voorwaarde van de federale politie: de bron correct vermelden zoals hier aangegeven. Die voorwaarde zegt niets over toegelaten gebruik buiten de bronvermelding — <em>niet</em> onder CC BY 4.0, en zonder uitdrukkelijke toelating voor commercieel hergebruik, in tegenstelling tot Statbel en de RVA hierboven. Elk van deze cijfers wordt berekend op de gemeentegrenzen die golden tot en met 2024, niet de huidige kaart, voor elk weergegeven jaar — ook jaren van ver voor die kaart bestond, want zo publiceert de bron zelf haar eigen geschiedenis. Het meest recente jaar van elke reeks wordt als voorlopig aangeduid.',
  },
};

/* Substitution, and a fallback to English rather than to the raw key.
   A French reader would rather see an English sentence than `communesLead`. */
I18N.t = function(lang, key, vars){
  const table = I18N.STRINGS[lang] || I18N.STRINGS[I18N.DEFAULT_LANG];
  const fallback = I18N.STRINGS[I18N.DEFAULT_LANG];
  let out = table[key] !== undefined ? table[key] : (fallback[key] !== undefined ? fallback[key] : key);
  for(const k in (vars || {})) out = out.split('{' + k + '}').join(vars[k]);
  return out;
};

/* The language to open in: an explicit choice first, then the browser's own
   preference, then English. Read from localStorage rather than the URL so a
   choice survives navigation between pages. */
I18N.initial = function(storage, navigatorLanguage){
  let saved = null, legacy = null;
  try{
    const store = storage || window.localStorage;
    saved = store.getItem(I18N.STORAGE_KEY);
    legacy = store.getItem(I18N.LEGACY_STORAGE_KEY);
  }catch(_){}
  if(I18N.LANGS.includes(saved)) return saved;
  if(I18N.LANGS.includes(legacy)) return legacy;
  const nav = (navigatorLanguage !== undefined ? navigatorLanguage
              : (typeof navigator !== 'undefined' ? navigator.language : '')) || '';
  return I18N.LANGS.find(l => nav.toLowerCase().startsWith(l)) || I18N.DEFAULT_LANG;
};

if (typeof module !== 'undefined') module.exports = I18N;
