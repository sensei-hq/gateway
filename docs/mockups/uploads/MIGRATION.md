# Migrating a hand-rolled design to UnoCSS utilities

Recipe used on the dbd landing page. Follow in order — steps 1–2 are where the
work is, steps 5–7 are the traps that cost the most time.

---

## 1. Build the config first, not the markup

Create `<design>/uno.config.js` as the **single source of truth**, shaped so the
app repo can consume the same file:

```js
(function () {
  var preflight = `…CSS variable blocks, keyframes, resets…`;
  var config = { theme: {…}, rules: […], shortcuts: {…}, preflights: [{ getCSS: () => preflight }] };
  if (typeof window !== 'undefined') window.__unocss = config;          // browser runtime
  if (typeof module !== 'undefined' && module.exports) module.exports = config; // build
})();
```

Fill `theme` by **auditing the old CSS, not by inventing a scale.** Grep every
literal in the old file and give each a semantic name:

| Old | New |
| --- | --- |
| `color: var(--muted)` / hexes | `theme.colors` — semantic only: `bg` `surface` `line` `fg` `muted` `faint` `accent` `action` `on-action` `tok-*`. Never `blue-500`. |
| `font-size: 0.72rem` | `theme.fontSize` — named steps (`eyebrow` `caption` `code` `body` `lede` `title` `head` `display`), each `[size, lineHeight]` |
| `letter-spacing: -0.035em` | `theme.letterSpacing` (`display` `head` `title` `brand` `eyebrow`) |
| `border-radius: 1.1rem` | `theme.borderRadius` |
| `max-width: 76rem` | `theme.maxWidth` |
| `padding: var(--section-y)` | `theme.spacing` — a var is a legal value: `{ section: 'var(--section-y)' }` |
| `@media (max-width: 960px)` | `theme.breakpoints` = the widths the design **already** uses, then `lt-lg:` / `lt-md:` / `lt-sm:` variants |

Rule of thumb: if the same 3+ utilities repeat on more than two elements, it is a
`shortcut` (`card`, `frame`, `btn-accent`, `eyebrow`, `h1`–`h4`, `grid-3`,
`shell`). If it appears once, leave it inline in the markup.

Anything utilities genuinely cannot express goes in the **preflight**, not in the
markup: theme variable blocks, `@keyframes`, `::selection`, body reset.

## 2. Convert the markup

Delete every inline style and every `@media` block. Convert mechanically, one
section at a time, and diff against the old render as you go.

Keep the theme switch as **data attributes + CSS variables** (`[data-theme="dark"]`,
`[data-accent]`, `[data-density]`) driving the token values. Do not build
`dark:` variants of every utility — one attribute swap re-themes the whole page,
and the density variable keeps working through `py-section`.

## 3. Self-host fonts via Fontsource

Replace the Google Fonts link with one stylesheet **per weight**, so what loads is
explicit and matches what the app installs:

```
dbd/fonts.css   →  @import url("https://cdn.jsdelivr.net/npm/@fontsource/<family>@5/latin-<weight>.css");
```

Then, in the config: map each family to its token (`font-display` / `font-sans` /
`font-mono`), constrain `theme.fontWeight` to only the shipped weights, and export
a `fonts` manifest (`{ pkg, family, token, weights }`) the dev can read directly.
Nothing in the design may use a weight outside that list.

## 4. Modularize

- One component per page segment (`SiteHeader`, `HeroSection`, … `SiteFooter`).
- Shared leaves for anything repeated with variants: `Button` (variant/size),
  `SectionHead`, `InfoCard`, `CodeFrame`. **Never hand-style a button again.**
- All copy/data in `content.js` (no markup); pure logic like a syntax
  highlighter in its own file, returning utility class names rather than colors.
- Each segment reads its own slice of the content module, so nothing is threaded
  through the root. The root owns only the theme shell and composition.
- Load the config in every component so each one previews standalone.

---

## 5. Traps — the reset

**A custom `preflights` array REPLACES the preset's reset.** Two separate bugs
came from this, and neither is obvious:

```css
*, ::before, ::after { box-sizing: border-box; border-style: solid; border-width: 0; }
```

- No `box-sizing` → every `w-full` + `px-*` container overflows its parent by the
  padding. Presents as "content clipped on the right".
- No `border-style` → **every** `border` utility silently collapses to nothing.
  Presents as "all my borders vanished" — and it's easy to misdiagnose, because
  `<button>` still shows a border from the UA stylesheet while divs and anchors
  don't.

Also put the base `font-family` on `body` in the preflight, not only on the root
component, or components previewed on their own fall back to serif.

## 6. Traps — specificity

Zero-specificity any preflight element selectors, or they outrank utilities:

```css
.root :where(a) { color: inherit; text-decoration: none; }
```

Without `:where()`, `.root a` (0,1,1) beats `.text-on-action` (0,1,0) and **every
text color utility on a link is silently ignored**. Same applies to any `p`, `ul`,
or `h*` rule you put in the preflight.

Define the light/default token block on `:root` as well as `[data-theme="light"]`,
and order it **before** the dark block — equal specificity means source order
decides, so a light block placed last kills dark mode.

## 7. Traps — the runtime

- `window.__unocss` must be assigned **before** the runtime script loads. Config
  script first, runtime second.
- The config is a plain script, so a **backtick inside the preflight template
  literal** (e.g. a CSS comment mentioning `` `preflights` ``) terminates the
  string and kills the whole file. Symptom: the entire page renders unstyled.
  Use quotes in CSS comments.
- Don't cache-bust with `?v=2` on a local config path — it 404s and the page
  renders unstyled.
- Runtime CSS is generated after the script parses the DOM, so expect a brief
  flash of unstyled content in the preview. A build-time Uno in the app doesn't
  have this; don't try to fix it in the design.
- If components mount inside wrapper elements, a wrapper sized to its content
  breaks `sticky` (the sticky child can't travel outside a 71px parent) and
  full-bleed backgrounds. Fix with `display: contents` on the mounts.

## 8. Verify by computed style, not by eye

The failures above look like design regressions and are invisible in a
screenshot until you know what you're looking at. After converting, assert:

- `document.documentElement.scrollWidth === clientWidth` (no overflow)
- a card's `borderTopWidth` / `borderTopStyle` is `1px solid`
- `boxSizing` is `border-box`
- an accent button's `color` in **both** themes
- `lt-*` variants generated real `max-width` media queries
- every custom property resolves (no empty `var()`)
