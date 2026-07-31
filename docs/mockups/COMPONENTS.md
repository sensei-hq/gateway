# gateway — handoff

## Artifacts
- `uno.config.js` — tokens, shortcuts, preflight, font manifest. Single source of truth;
  exports on `window.__unocss` (runtime) and `module.exports` (build). Import unchanged.
- `fonts.css` — one Fontsource stylesheet per shipped weight (= the font manifest in CSS form).
- `content.js` — all copy and data. Each segment reads its own slice; nothing is threaded
  through the root.
- Components: one `.dc.html` per page segment + shared leaves.

## Font manifest
| package | family | token | weights |
| --- | --- | --- | --- |
| `@fontsource/space-grotesk` | Space Grotesk | `font-display` | 500, 600 |
| `@fontsource/ibm-plex-sans` | IBM Plex Sans | `font-sans` | 400, 500, 600 |
| `@fontsource/jetbrains-mono` | JetBrains Mono | `font-mono` | 400, 500, 600 |

## Theme axes (data attributes + CSS variables, no `dark:` variants)
`[data-theme]` light | dark · `[data-accent]` blue | violet | teal · `[data-density]` default | compact.
Set on the root shell; the versioning panel nests its own `[data-theme]` to invert.
Derived accent tokens are re-declared on every themed subtree so nesting re-derives correctly.

## Component inventory
| name | props | used in |
| --- | --- | --- |
| `Button` | `variant` primary/secondary/ghost, `size` sm/md/lg, `label`, `href`, `arrow` | header, hero, CTA |
| `Chip` | `label`, `tone` gray/accent | crates |
| `Eyebrow` | `label` | SectionHead |
| `SectionHead` | `eyebrow`, `title`, `body`, `mono`, `align`, `width` | every section |
| `InfoCard` | `tag`, `title`, `body` | features |
| `CodeFrame` | `filename`, `code`, `copyable`, `tabs`, `activeTab`, `onSelectTab` | hero, usage |
| `SiteHeader` | `theme`, `onToggleTheme` | root |
| `HeroSection` / `ProofStrip` / `FeaturesSection` / `CratesSection` / `UsageSection` / `ArchitectureSection` / `ConsumersSection` / `CtaSection` / `SiteFooter` | — (read `content.js`) | root |
| `VersioningSection` | `theme` (inverts it) | root |
| `Gateway Site` | `theme`, `accent`, `density` | entry |

## Runtime-integration only — no-ops in a real build
- `.sc-host { display: contents }` in the preflight: the design host wraps each mounted
  component in a `div`, which would otherwise break `sticky` and the full-bleed bands.
- Config script must be loaded **before** the Uno runtime script in every component's head.
- Brief flash of unstyled content in preview: runtime generates CSS after parsing the DOM.
  A build-time Uno does not have this — do not "fix" it.
- The preflight is assembled from an array of lines, not a template literal, so a stray
  backtick can never kill the file.

## Known deltas from the pre-migration design
- Body copy stepped up to 16px (`text-body`) per the ≥16px minimum; the old 15px step
  survives as `text-sub` for dense card/nav copy.
- Em dashes in copy were normalised to hyphens in `content.js` — swap them back there if
  you want typographic dashes.
- `theme.fontWeight` adds the three shipped weights but the preset's other weights are
  still reachable (deep theme merge). Constrain in the app build if you want it enforced.
