# Feature: site unification — one coherent public interface across seven pages

Status: approved by the maintainer 2026-09-14. Delivery A (interface) in progress. Delivery B
(Europe map) awaits data approval -- see Batches below.

Issue: none yet

Branches: one per batch -- feat/unify-* for Delivery A, feat/europe-* for Delivery B.

Batches:

- **A0** -- LF-only page writers, .gitattributes, and this spec.
- **A1.1** -- shared header/footer fragments rendered at generation time, plus a deterministic
  command that syncs only the delimited header/footer zones of hand-edited pages.
- **A1.2** -- themes Clair, Sombre and Papier, plus the shared theme/language icon menus.
- **A1.3** -- BPCharts shared interactive points and tooltips, plus Home2's map/legend/source
  composition fix.
- **A1.4** -- Macro's seven thematic panels (Vue d'ensemble, Croissance, Prix, Emploi,
  Conjoncture, Finances publiques, Europe).
- **A2** -- visual gate on the two pilots (Home2, Macro) with real data, before the pattern
  spreads further.
- **A3.1** -- the other five pages (Profils, Commune, Micro, Cartes, Sources) plus Micro's seven
  panels.
- **A3.2** -- the commune journey: every selection opens `commune.html?nis=xxxxx`,
  `profiles.html?nis=xxxxx` redirects there, plus a full link audit.
- **A4** -- finish Delivery A against the spec's own acceptance list.
- **B1** -- NUTS 2 coverage report (GDP per capita, unemployment, population), a data spec, and
  proposed docs/data_catalog.md rows for maintainer approval.
- **B2** -- pipeline extensions and published regional payload files.
- **B3** -- the Europe panel UI (NUTS 2 map), gated on A's pilots being stable and B1/B2 being
  approved.

## Limits of the mockups

The maquettes at `docs/design-references/maquettes-unification-v1/` (see `prompts.md` for the
prompts that produced them, and `index.html` for the gallery) are illustrative only. The figures,
chart outlines, category names and cited sources shown in the images are placeholders from the
mockup-generation process, not real BelPulse data. Every real batch reads its figures, categories,
units and sources from the pipeline's own metadata (`public/data/metadata/**`,
`config/indicators/*.yaml`) per CLAUDE.md rules 2, 28 and 36 -- the mockups fix layout and
interaction pattern, never a number.

The nine mockup screenshots (`01-accueil.png` … `09-themes.png`, roughly 14 MB total) that
`prompts.md` and `index.html` reference are **committed with maintainer approval, 2026-09-14**
(CLAUDE.md rule 12 requires that go-ahead before committing new binary artefacts; each file is
under 2 MB, well below the 25 MB per-file limit).

---

*What follows is the maintainer's own brief, received 2026-09-14, kept verbatim as this spec's
body.*

# BelPulse : une interface cohérente, compacte et interactive

(Brief du mainteneur, reçu le 2026-09-14. Maquettes : docs/design-references/maquettes-unification-v1/.)

## 1. Résultat attendu et méthode

Les sept pages publiques — Accueil, Profils, Commune, Macro, Micro, Cartes et Sources — doivent donner l'impression d'appartenir au même produit.

Conserver les choix validés :

- présentation compacte, largeur maximale de 1120 px ;
- Inter pour l'interface, Spectral pour les titres ;
- thèmes Clair, Sombre et Papier ;
- menus à icônes pour le thème et la langue ;
- Macro/Micro organisés en rubriques sélectionnables ;
- carte européenne NUTS 2 avec PIB par habitant, chômage et population.

**Commencer par Home2 et Macro comme pages pilotes.** Elles couvrent les deux dispositions nécessaires : page éditoriale et tableau de bord avec menu latéral. Une fois leur présentation vérifiée, appliquer les mêmes composants aux autres pages.

Travailler dans un worktree dédié depuis la dernière base intégrée. Relever d'abord les changements déjà apportés par Claude pour éviter de refaire ou d'écraser son travail.

Deux livraisons : l'interface complète, puis la carte européenne alimentée par le pipeline. Le chantier européen reste inclus dans l'objectif.

## 2. Une présentation partagée, appliquée aux sept pages

### Header, footer et styles

Réutiliser les styles et renderers existants. Centraliser le markup du header/footer dans des fragments rendus à la génération : les pages publiées contiennent leur navigation directement, sans attendre une requête JavaScript.

Pour les pages HTML éditées directement, une commande déterministe synchronise uniquement les zones explicitement délimitées du header/footer. Les pages issues du générateur consomment les mêmes fragments. Ne pas convertir tout le site en nouveau système de templates.

Un seul module partagé gère les menus, la langue et le thème.

Règles visuelles communes :

- contenu centré, maximum 1120 px ;
- texte principal 14 px, métadonnées 12 px minimum ;
- titres hiérarchisés et mêmes tailles de boutons, champs et espacements ;
- cartes dimensionnées par leur contenu ;
- zones tactiles suffisamment grandes malgré la présentation compacte ;
- retour à la ligne des intitulés complets.

Les bandeaux de Home2 peuvent conserver un fond pleine largeur. Macro/Micro utilisent la même largeur totale, menu compris.

### Navigation et parcours communal

Header et footer proposent : Accueil, Profils communaux, Macro, Micro, Cartes, Sources.

Les liens de contenu suivent la même liste, avec `commune.html` comme destination des fiches. Conserver les liens externes de sources, les licences et téléchargements.

Remplacer les destinations obsolètes par leur équivalent réel ; supprimer les boutons sans fonction utile. Vérifier les liens créés dynamiquement et ceux de la recherche.

Dans Profils :

- conserver les filtres par famille, région, province et type ;
- toute sélection d'une commune ouvre `commune.html?nis=xxxxx` ;
- rediriger les anciennes URL `profiles.html?nis=xxxxx` ;
- retirer la vue communale redondante de Profils après vérification de la présence de ses informations et fonctions dans Commune.

Les anciennes pages restent accessibles par leur URL, mais disparaissent de ce parcours public.

### Thèmes, langues et intitulés

Deux petits menus :

- thème : Clair, Sombre, Papier ;
- langue : Français, Nederlands, English, avec code courant visible.

Conserver les préférences entre pages et les appliquer avant le premier affichage. Migrer une ancienne préférence « auto » vers son apparence effective, puis enregistrer un choix explicite.

Le thème Papier est une palette partagée : fond crème légèrement quadrillé, accents pastel, texte sombre et tracés nets. Il garde toutes les fonctionnalités. Le quadrillage reste en arrière-plan et ne surcharge pas les graphiques.

Les noms d'indicateurs, unités, légendes et infobulles utilisent les métadonnées traduites existantes. Corriger les traductions manquantes à leur source. Aucun libellé statistique ne doit dépendre uniquement d'une infobulle pour être lu entièrement.

## 3. Interactions et organisation des pages

### Graphiques : améliorer une seule couche

Étendre `BPCharts` avec une gestion commune des points interactifs et des infobulles. Conserver ses fonctions existantes pour limiter les régressions.

Chaque graphique doit permettre de consulter : la série ou le territoire ; la période ; la valeur et l'unité ; le statut particulier, lorsqu'il existe.

Survol à la souris, sélection au toucher et accès aux valeurs au clavier ou par une table associée. Les graphiques spécifiques, comme la pyramide des âges, réutilisent le même affichage d'infobulle.

Le redimensionnement et les changements de thème/langue redessinent le graphique. Les séries sont alignées sur leurs périodes réelles, et les valeurs manquantes ne deviennent jamais zéro.

### Home2 : corriger la composition

Séparer matériellement les zones de la carte : 1. titre et choix de l'indicateur ; 2. carte et commandes de zoom ; 3. légende et source.

Réserver l'espace des commandes au lieu de les superposer au sélecteur. Ajuster les contours à la surface disponible avec proportions conservées.

Pour les deux graphiques de droite, utiliser une structure commune : titre complet, dernière valeur et période, tracé, source. Donner au tracé une hauteur propre et le dessiner à sa taille réelle.

À petite largeur, empiler les blocs avant qu'ils deviennent illisibles. Ne pas chercher à maintenir trois colonnes à tout prix.

### Macro/Micro : panneaux thématiques

Utiliser des panneaux HTML dans la page, sans iframe ni nouveau framework. Un seul panneau est visible.

Macro : Vue d'ensemble, Croissance, Prix, Emploi, Conjoncture, Finances publiques, Europe.
Micro : Vue d'ensemble, Population, Revenus, Emploi, Logement, Entreprises, Comparaisons territoriales.

Le menu reste collant sous le header sur ordinateur ; sur mobile, il devient un sélecteur de rubrique. Le contenu conserve le défilement normal du document.

Chaque rubrique dispose d'une ancre partageable, compatible avec précédent/suivant. Les anciennes ancres rejoignent leur rubrique équivalente. Les graphiques sont initialisés ou redimensionnés après affichage du panneau.

Reclasser les données par configuration, sans formules ni identifiants statistiques ajoutés dans les composants génériques. Conserver les informations existantes ; une rubrique sans données affiche une explication concise.

## 4. Europe : une carte régionale réellement alimentée

Intégrer une carte NUTS 2 dans le panneau Europe de Macro, avec trois indicateurs : PIB par habitant en pouvoir d'achat ; taux de chômage ; population.

Utiliser `eurostat-map` pour cette carte européenne, après vérification de la version et des conditions de distribution. Les cartes communales conservent leur moteur partagé.

### Une seule chaîne de données

Réutiliser le connecteur Eurostat/DBnomics existant lorsqu'il couvre les jeux régionaux requis. Ajouter uniquement les éléments manquants.

Avant l'interface, produire un petit rapport de couverture sur les trois jeux : dimensions exactes, unité, années, régions disponibles et version NUTS. Enregistrer les sources et géométries dans le catalogue selon les règles du dépôt.

Publier des fichiers dédiés au niveau régional. Le navigateur consomme ces fichiers générés ; il ne télécharge pas les statistiques directement chez Eurostat.

Ne pas confondre codes NUTS et NIS. Ne pas appliquer des valeurs à des contours de millésime incompatible.

### Expérience utilisateur

- choix de l'indicateur et de l'année ; même année pour toutes les régions affichées ; dernière année disponible de l'indicateur par défaut ;
- légende, zoom, réinitialisation, survol et sélection d'une région ; état visible pour les données absentes ;
- source, unité et millésime géographique affichés ; détail de la région dans le panneau existant, sans nouvelle page publique.

Charger les ressources européennes uniquement à l'ouverture du panneau. Conserver une classification identique entre thèmes ; seules les couleurs changent.

Les trois indicateurs font partie de la livraison. Une indisponibilité upstream doit être documentée précisément et signalée comme travail partiel, sans présenter une carte vide comme terminée.

## 5. Vérification et ordre de livraison

Livraison A — interface : 1. composants communs sur Home2 et Macro ; 2. vérification visuelle avec données réelles ; 3. application aux cinq autres pages ; 4. parcours communaux, panneaux et interactions.

Livraison B — Europe : 1. vérifier les jeux et géographies ; 2. générer et valider les payloads ; 3. brancher la carte sur les composants d'interface stabilisés.

Tests proportionnés. Étendre les tests existants de navigation, thème et graphiques. Ajouter des tests de comportement pour les nouveaux panneaux et la redirection communale. Automatiser : liens autorisés et destinations des communes ; conservation de la langue, du thème et de la rubrique ; exactitude des valeurs des infobulles ; absence de débordement et de chevauchement ; couverture et correspondance des codes NUTS.

Revue visuelle des sept pages à 390, 768 et 1122 px. Tester toutes les combinaisons langue/thème sur les deux pages pilotes ; vérifier les libellés longs et les composants propres aux autres pages.

Tests ciblés pendant le travail, puis suite complète avant chaque livraison. Une reconstruction conserve l'interface et ne modifie pas les statistiques belges existantes. Captures et résultats dans les PR. Actualiser l'avancement uniquement pour les fonctions effectivement vérifiées.

## Décisions prises avec le mainteneur (2026-09-14)

- Ancres des rubriques en français, comme les maquettes (#apercu, #croissance, #prix, #emploi, #conjoncture, #finances-publiques, #europe ; micro : #apercu, #population, #revenus, #emploi, #logement, #entreprises, #comparaisons). Les anciennes ancres anglaises redirigent.
- Le volet données de la livraison B (rapport de couverture + lignes du catalogue, puis pipeline) avance en parallèle de A. Seule la carte (B3) attend l'interface stabilisée. Le pipeline (B2) attend l'approbation des lignes du catalogue (règle 8).
