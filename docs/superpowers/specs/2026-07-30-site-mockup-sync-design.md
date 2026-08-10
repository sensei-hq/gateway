# Design — Sync `site/` to the mockups + unit tests

- **Date:** 2026-07-30
- **Status:** Approved (design); pending implementation plan
- **Scope:** `site/` (SvelteKit + Rokkit marketing website) only. No Rust/crate changes.

## 1. Problem

The marketing site (`site/`) is a simplified, partially-drifted implementation of the
design in `docs/mockups/`. Three gaps:

1. **Not modularized to the mockup breakdown.** All page segments are inlined in a
   197-line `site/src/routes/+page.svelte`. The mockups decompose the page into one
   block per segment (`HeroSection` … `SiteFooter`) plus shared leaves.
2. **Naming diverges** from the mockup's 1:1 handoff inventory (`CodeWindow` vs
   `CodeFrame`, `FeatureCard` vs `InfoCard`, `Nav` vs `SiteHeader`, `Footer` vs
   `SiteFooter`).
3. **No unit tests exist** in `site/` (no Vitest, no testing-library, no test scripts).
   The lower-fidelity `ArchDiagram` also lags the mockup's animated d3 diagram.

## 2. Source-of-truth principle (decided)

- **Mockups** (`docs/mockups/*.dc.html`, `COMPONENTS.md`, `DESIGN_PRINCIPLES.md`) are the
  reference for **visual styling** and the **component/block decomposition** (which
  components exist, their props, how the page splits).
- **The existing app** (`site/`) is the source of truth for **everything else**:
  - **Data / content** — `data.ts`, which is code-derived (version from `package.json`;
    three crates `gateway` / `local-providers` / `local-engine`; ~16 providers;
    capability traits). The mockup's `content.js` is stale (`v0.2.18`, two crates,
    `InferenceAdapter`) and is **not** a content source.
  - **Icons** — `BrandMark`, `ArrowIcon`, `@rokkit/icons`. Not the mockup's letter-glyph
    marks.
  - **Styling system** — the app's Rokkit/UnoCSS token vocabulary (`bg-accent-soft`,
    `text-ink`, `border-paper-edge`, existing `--code-*` CSS vars). The mockup is a
    visual reference, not a token source; no migration to the mockup's raw
    `uno.config.js`.
  - **Behavior & stack** — SvelteKit routing, `vibe`/`themable` theme shell, Cloudflare
    adapter, Bun as package manager.

## 3. Decisions locked

| Fork | Decision |
|---|---|
| Component naming | **Adopt mockup names 1:1** (rename to the handoff inventory). |
| Unit-test stack | **`vitest-browser-svelte`** (real browser via Playwright). |
| Architecture diagram | **Port the mockup's animated d3 SVG** (styled per mockup, data from code, icons from app). |

## 4. Target component tree

### 4.1 Rename / restructure map

| Today (`site/src/lib/components/`) | Becomes | Kind |
|---|---|---|
| `Nav.svelte` | **`SiteHeader.svelte`** | block (rendered by `+layout.svelte`) |
| `Footer.svelte` | **`SiteFooter.svelte`** | block (rendered by `+layout.svelte`) |
| `FeatureCard.svelte` | **`InfoCard.svelte`** | leaf |
| `CodeWindow.svelte` + `UsageTabs.svelte` | **`CodeFrame.svelte`** (merged) | leaf |
| `ArchDiagram.svelte` | d3 canvas inside **`ArchitectureSection.svelte`** (kept as an app-internal `ArchDiagram.svelte` child for testability) | block + internal leaf |
| `SectionHead`, `Eyebrow`, `Chip`, `CrateCard`, `ArrowIcon`, `BrandMark`, `Seo` | unchanged | leaves |
| `@rokkit/ui` `Button` | unchanged — the single Button implementation | external |

### 4.2 New section blocks (extracted from `+page.svelte`)

One block per page segment, each reading its own `data.ts` slice (no props threaded
through the root):

`HeroSection`, `ProofStrip`, `FeaturesSection`, `CratesSection`, `UsageSection`,
`ArchitectureSection`, `ConsumersSection`, `VersioningSection`, `CtaSection`.

Plus `SiteHeader` and `SiteFooter` (renamed from `Nav`/`Footer`), which stay in
`+layout.svelte`.

### 4.3 Composition split

- **`+layout.svelte`** — owns the theme shell (`vibe`, `use:themable`, font imports) and
  renders `SiteHeader` / `<main>` / `SiteFooter`. Unchanged responsibility; only the two
  renames.
- **`+page.svelte`** — becomes composition-only: `<Seo/>` + the section blocks in order.
  Target: well under 40 lines, no inlined section markup.

## 5. `CodeFrame` consolidation

Single leaf replacing both `CodeWindow` and `UsageTabs`, matching the mockup
`CodeFrame.dc.html` contract:

- **Props:** `{ filename?: string; code: string; copyable?: boolean; tabs?: Array<{ id: string; label: string; code: string }>; activeTab?: string; onSelectTab?: (id: string) => void }`
- **No `tabs`** → traffic-light chrome + `filename` + copy button (today's `CodeWindow`).
- **With `tabs`** → tablist bar with `role="tablist"` / `role="tab"` / `aria-selected`,
  active-tab state (uncontrolled default via first tab; controllable via
  `activeTab`/`onSelectTab`) (today's `UsageTabs`).
- Reuses the app's existing `--code-*` styling; mockup is the visual target.
- Copy uses `navigator.clipboard.writeText` guarded in try/catch with a 1.5s "Copied"
  state (existing behavior preserved).

Call sites: hero code block and `UsageSection` both use `CodeFrame`.

## 6. `ArchitectureSection` — d3 port

- Add `d3` as a `site/` dependency; render into `ArchDiagram.svelte` (SVG via d3, mounted
  with an `$effect`, `viewBox="0 0 1080 440"`, `preserveAspectRatio`).
- Reproduce the mockup: app/engine/adapter/backend cards, curved flow links with an
  arrow marker, the animated dash "flow" overlay, capability rows, and provider/backend
  pills.
- **Colors come from the app's CSS-var tokens** (e.g. `var(--c-accent)` equivalents in the
  app's Rokkit token set) so the diagram re-themes with light/dark — never hard-coded hex.
- **Data from code:** add an `architecture` diagram slice to `data.ts` reflecting current
  reality — engine caps `[fallback, circuit breaker, budget, tracing]`; notes
  `no DB · GatewayStore trait`, `reqwest · rustls · tokio`; cloud `~16 adapters` with the
  provider pills already in `data.ts`; local backends `llama.cpp · onnx · fastembed`;
  adapter label `capability traits`.
- **Accessibility / motion:** respect `prefers-reduced-motion` — freeze the flow-dash
  animation when reduced motion is requested. Provide an accessible label/`role="img"`
  with a text description on the SVG.

## 7. Data changes (`data.ts`)

Additive only — no content rewrites:

- Add an exported `architecture` diagram slice (section 6) with a typed shape.
- Keep every existing export and its code-derived values (`VERSION`/`TAG` from
  `package.json`, three crates, provider list, etc.).

## 8. Testing — `vitest-browser-svelte`

### 8.1 Harness

- **Add devDeps** to `site/`: `vitest`, `@vitest/browser`, `vitest-browser-svelte`,
  `playwright` (Chromium).
- **Configure** a `test` block in `site/vite.config.ts`: browser mode, provider
  `playwright`, `headless: true`, instance Chromium; include `src/**/*.{test,spec}.ts`.
  Ensure the UnoCSS/SvelteKit plugins apply so components render with real styles (needed
  for computed-style assertions).
- **Scripts** (run with **bun** per the repo package-manager rule): `test` →
  `vitest run`, `test:watch` → `vitest`. Wire into the site `check` flow where sensible.

### 8.2 Coverage — one test file per component/block

For **every leaf and block** (`SiteHeader`, `HeroSection`, `ProofStrip`, `FeaturesSection`,
`CratesSection`, `UsageSection`, `ArchitectureSection`, `ConsumersSection`,
`VersioningSection`, `CtaSection`, `SiteFooter`, `InfoCard`, `CodeFrame`, `SectionHead`,
`Eyebrow`, `Chip`, `CrateCard`, `ArrowIcon`, `BrandMark`, `ArchDiagram`, `Seo`):

- **Renders standalone** (design principle: every component renders on its own).
- **Props / variants** — e.g. `SectionHead` align/width, `Chip` tone, `CodeFrame`
  chrome-vs-tabs, `InfoCard` tag/title/body.
- **Data-driven rendering** — blocks render the expected count/labels from their `data.ts`
  slice (`{#each}` output).
- **Accessibility** — roles/labels (`role="tablist"`/`aria-selected` on `CodeFrame`,
  nav landmarks on `SiteHeader`, `aria-label`/`role="img"` on icons and `ArchDiagram`,
  `alt` where applicable).
- **Computed-style assertions** (the mockup's "verify by computed style, not by eye"
  rule) on a representative sample: no horizontal overflow
  (`scrollWidth === clientWidth`), a card border computes `1px solid`,
  `box-sizing: border-box`, and Button/label color correct in **both** light and dark
  themes.
- **`CodeFrame` copy** — stub `navigator.clipboard`; assert label toggles to "Copied".
- **`ArchDiagram`** — assert the SVG structure it emits (a `<svg>` with the expected
  card/pill/link nodes and the flow marker), and that reduced-motion disables the dash
  animation.

Tests are unit-level and fast; behavior-preserving renames (steps 2–3) start from
green tests written against current behavior.

## 9. Sequencing (TDD, small single-purpose commits, green at each step)

1. **Harness** — add vitest-browser-svelte config + one smoke test → green baseline.
2. **Leaves** — consolidate with behavior identical, each with tests: `InfoCard` ←
   `FeatureCard`; `CodeFrame` ← `CodeWindow` + `UsageTabs`; add tests for existing
   `SectionHead` / `Eyebrow` / `Chip` / `CrateCard` / `BrandMark` / `ArrowIcon` / `Seo`.
3. **Blocks** — extract each section block one at a time (+ tests); slim `+page.svelte`
   to composition; rename `Nav` → `SiteHeader`, `Footer` → `SiteFooter` in the layout.
4. **Arch** — port the d3 `ArchitectureSection` (+ tests); add the `architecture` slice.
5. **Verify** — `svelte-check`, full test suite green, computed-style checks, `bun run
   build` succeeds.

Each numbered step is its own commit (or small set), keeping the pipeline green
throughout (never merge on red; a human reviews before it lands).

## 10. Non-goals

- No content rewrite (data stays code-derived; mockup `content.js` is ignored).
- No migration to the mockup's raw `uno.config.js` token names.
- No new pages or routes; no changes to `/docs`, `llms.txt`, or SEO behavior. The `Seo`
  component is kept as-is (no rename, no prop changes).
- No Rust/crate changes.
- No e2e or visual-regression suite (unit-level component tests only).

## 11. Definition of done

- Page decomposed into the mockup's block/leaf inventory; `+page.svelte` is
  composition-only.
- 1:1 mockup names in place (`SiteHeader`, `SiteFooter`, `InfoCard`, `CodeFrame`, section
  blocks).
- `CodeFrame` serves both chrome and tabbed call sites; `CodeWindow` + `UsageTabs`
  removed.
- `ArchitectureSection` renders the d3 diagram, re-themes via tokens, respects
  reduced-motion, and reads its data from `data.ts`.
- Every component/block has a unit test; `vitest` suite green; `svelte-check` clean;
  `bun run build` succeeds.
- Content unchanged and still code-derived.

## 12. Traps to respect (from `docs/mockups/uploads/MIGRATION.md`)

The mockup's reset/specificity/runtime traps (`box-sizing`, `:where()` zero-specificity,
`window.__unocss` load order, backticks in the preflight, `display: contents` on mounts)
are **runtime-integration-only** and are no-ops in the app's build-time UnoCSS — do not
cargo-cult them. The relevant carry-over is behavioral: assert by computed style, keep the
d3 diagram's colors as CSS-var tokens so themes resolve, and ensure no horizontal overflow.
