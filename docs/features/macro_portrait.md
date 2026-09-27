# Macro Portrait

Requested 2026-09-27: copy the shipped commune Portrait design onto macro.html.

Reuse commune.html's hero typography, topic pills and essentials-band styling.
Keep the existing national payloads, multilingual labels, chart renderer, seven
panels, mobile picker, history/hash navigation, and unavailable states. The
existing skyline fills the visual slot; no new image source is introduced.
The title and freshness remain visible above every panel. Styling is scoped in
assets/belpulse/macro_portrait.css so the commune page is unaffected.

Verify desktop and phone layouts, three languages and themes, populated KPIs,
and switching to a chart panel with a nonzero canvas width. Existing panel
contracts continue to run on CI.
