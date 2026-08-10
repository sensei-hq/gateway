# Design guiding principles

Front-load these and there is no migration later. Written for design work that
gets handed to engineers building with UnoCSS/Tailwind.

Companion doc: `MIGRATION.md` covers converting an existing hand-rolled design.
This doc is what you read **before** the first screen exists.

---

## 0. Decide these before any markup

Write the answers down. Rework almost always traces back to one of these being
decided implicitly, mid-build, in two places at once.

1. **Palette** — 1 background tone, 1 surface, 1 line, 3 text weights
   (`fg` / `muted` / `faint`), 1 accent, 1 action (button) pair. That's it.
2. **Type** — 1–3 families max, and the **exact weights** you will ship.
3. **Type scale** — the named steps, with line-heights attached.
4. **Spacing rhythm** — the section padding steps and the card padding step.
5. **Breakpoints** — the 2–3 widths this design actually reflows at.
6. **Radii** — usually 2: control radius and card radius.
7. **Theme axes** — light/dark? accent choices? density? Each is a data attribute,
   decided now, not retrofitted.
8. **Motion** — one entrance easing/duration, and reduced-motion behavior.

Anything not on this list is a one-off and belongs inline at its single call site.

## 1. The config is the design system

One file, at the start, consumable by both the design and the app repo. Tokens,
shortcuts, and a preflight. If a value exists in two places, it is already a bug.

- **Semantic names, never descriptive ones.** `surface`, `line`, `muted`,
  `action`, `on-action`. Never `gray-200`, `blue-500`, `text-sm-2`. The dev must
  be able to swap a token's value without renaming call sites.
- **Every color pairs with its foreground.** `action` + `on-action`,
  `accent` + `on-accent`. Contrast is decided in the config, once — not judged
  per button. If a fill changes per theme, its label token changes with it.
- **Named type steps with line-height baked in.** `text-lede`, `text-title`,
  `text-head`. A designer should never write a raw `rem` in markup.
- **Constrain `fontWeight` to the weights you actually ship.** This makes an
  unshipped weight impossible to use by accident.
- **Theme via data attributes + CSS variables.** `[data-theme]`, `[data-accent]`,
  `[data-density]` swap variable values. Do **not** author `dark:` variants of
  every utility — one attribute re-themes everything, and density/accent come
  free through the same mechanism.
- **A variable is a legal token value.** `spacing: { section: 'var(--section-y)' }`
  lets a density switch drive layout with no extra classes.

## 2. Utilities in markup. Nothing else.

No inline styles, no stylesheets, no CSS classes of your own.

- **Rule of three:** the same 3+ utilities on more than two elements becomes a
  `shortcut`. Once is inline. This is the whole componentization rule for CSS.
- **Responsive is variants only** (`lt-md:`, `md:`) — never a media query, and
  never a second "mobile" component.
- **Only these live in the preflight:** token variable blocks, `@font-face`,
  `@keyframes`, `::selection`, body reset. Everything else is a utility.
- **Zero-specificity any element selector you put there:**
  `:where(a)`, not `a`. Otherwise it outranks utilities and silently wins.
- **Layout is flex/grid + `gap`.** Never margins between siblings, never
  whitespace-as-spacing — gap survives reordering and deletion.

## 3. Components: shallow, few, prop-driven

- **One component per page segment**, plus shared leaves for anything repeated
  with variants. Resist deeper trees; 400 lines in one segment is fine.
- **Exactly one Button, one SectionHead, one Card family.** The moment a second
  button is hand-styled, consistency is gone and no config can recover it.
- **Variants are enumerated props** (`variant`, `size`, `align`), never free-form
  class strings passed in from outside. The variant list *is* the contract with
  the dev.
- **Content lives in a data module**, not in markup. Copy edits then never touch
  layout, and translation/CMS work is a swap.
- **Pure logic goes in its own file** and returns *token names*, not colors — a
  highlighter emits `text-tok-key`, never `#0b6`.
- **Each segment reads its own data slice.** Don't thread props through the root;
  the root owns the theme shell and composition, nothing else.
- **Every component must render standalone.** If it only looks right nested in
  the page, its dependencies are implicit and the dev will hit the same wall.

## 4. Name things the way the dev will

Component and prop names carry over verbatim, so choose them as if writing React:
`Button`, `SectionHead`, `InfoCard`, `CodeFrame`; props `variant`, `size`,
`align`, `label`, `href`. A 1:1 name map is most of what makes handoff fast — no
translation layer, no "which card is this?" conversations.

## 5. Handoff artifacts

Ship these alongside the design, not after someone asks:

- `uno.config.js` — tokens, shortcuts, preflight. Exports for both runtime and build.
- `fonts.css` + a **font manifest** — package, family, token, exact weights.
- `content.js` — all copy and data.
- A one-screen **component inventory**: name, props, where used.
- A note on which rules are *runtime-integration only* and are no-ops in a real
  build, so the dev doesn't cargo-cult them.

## 6. Accessibility and minimums, decided up front

- Contrast is a config decision (see the pairing rule), verified once per theme.
- Hit targets ≥ 44px on touch layouts.
- Body text ≥ 16px; never below 12px anywhere.
- Respect `prefers-reduced-motion` in the same rule that defines the animation.
- Focus states are part of the Button contract, not an afterthought.

## 7. Verify by computed style, not by eye

Reset/specificity failures look exactly like design regressions and survive
screenshot review. Before calling anything done, assert:

- `scrollWidth === clientWidth` — no horizontal overflow
- a card computes `1px solid` (borders don't silently collapse)
- `box-sizing: border-box` everywhere
- button `color` correct in **every** theme
- responsive variants generated real media queries
- no unresolved `var()` — every custom property has a value
- one component rendered standalone still looks right

---

## Definition of done

- [ ] No inline styles, no media queries, no bespoke CSS classes
- [ ] No raw color, font-size, or radius literal anywhere in markup
- [ ] Every repeated composition is a shortcut; every repeated element a component
- [ ] Only one button implementation exists
- [ ] All copy sits outside markup
- [ ] Config is importable by the app repo unchanged
- [ ] Shipped font weights match the manifest exactly
- [ ] All themes verified by computed style, not screenshot
- [ ] Every component renders correctly on its own
