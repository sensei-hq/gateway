# Site Mockup Sync + Component Unit Tests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Modularize the `site/` marketing page to the mockups' block/leaf inventory (1:1 names), consolidate `CodeWindow`+`UsageTabs` into `CodeFrame`, port the animated d3 architecture diagram, and add a `vitest-browser-svelte` unit test for every component/block.

**Architecture:** SvelteKit + Rokkit (Svelte 5) app. `+layout.svelte` owns the theme shell + `SiteHeader`/`SiteFooter`; `+page.svelte` becomes composition-only, rendering one block per page segment, each reading its own `data.ts` slice. Styling stays in the app's Rokkit/UnoCSS token vocabulary; the mockups are the visual reference. Content is code-derived (`data.ts`), never from the stale `content.js`.

**Tech Stack:** SvelteKit 2, Svelte 5 (runes), UnoCSS + presetRokkit, `@rokkit/*`, `d3-selection`, Vitest browser mode via `vitest-browser-svelte` + Playwright (Chromium). Package manager: **Bun** (`bun run …`). Spec: `docs/superpowers/specs/2026-07-30-site-mockup-sync-design.md`.

**Conventions used throughout:**
- Component tests are named `<Name>.svelte.test.ts`, co-located in `site/src/lib/components/`.
- Run a single test file: `cd site && bun run test -- src/lib/components/<Name>.svelte.test.ts` (Vitest `run` mode).
- Run the whole suite: `cd site && bun run test`.
- Commit after each task once its tests are green and `bun run check` passes.
- All `git`/`bun` commands run from the repo root unless a `cd site` is shown.

**Test API (confirmed against `vitest-browser-svelte@3.0.0` + `@vitest/browser@4.1.10` in Task 0.3):**
- `render()` is **async** — always `const screen = await render(Component, props?)`. (The draft snippets in later tasks show `const screen = render(...)`; add `await`.)
- `screen` exposes `.container` (raw `HTMLElement`) plus Testing-Library locator methods spread on it (`getByRole`, `getByText`, `getByLabelText`, `getByTestId`, …).
- `expect.element(locatorOrElement)` is globally augmented (retrying); `locator.element()` returns the raw node; `locator.click()` interacts. No jest-dom import needed.
- Cleanup between tests is automatic (the package registers a `beforeEach` unmount).
- The browser project prints as `client (chromium)` — grep for it to confirm a test really ran in the browser.
- Failure artifacts (`.vitest-attachments`, `**/__screenshots__`) are gitignored in `site/.gitignore`.

---

## Phase 0 — Test harness

### Task 0.1: Install test dependencies

**Files:**
- Modify: `site/package.json` (via `bun add`)

- [ ] **Step 1: Add dev dependencies (Bun, in `site/`)**

```bash
cd site
bun add -d vitest @vitest/browser vitest-browser-svelte playwright d3-selection @types/d3-selection
```

- [ ] **Step 2: Install the Chromium browser Playwright drives**

```bash
cd site
bunx playwright install chromium
```

Expected: Chromium downloads to the Playwright cache (one-time).

- [ ] **Step 3: Commit**

```bash
git add site/package.json site/bun.lock
git commit -m "chore(site): add vitest-browser-svelte test dependencies"
```

### Task 0.2: Configure Vitest (client browser + server node projects)

**Files:**
- Modify: `site/vite.config.ts`
- Create: `site/vitest-setup-client.ts`
- Modify: `site/package.json` (scripts)

- [ ] **Step 1: Rewrite `site/vite.config.ts` to add the `test` block**

Keep the existing version `define`; switch `defineConfig` to `vitest/config` and add two test projects. The `client` project runs component tests (`*.svelte.test.ts`) in a real browser; the `server` project runs plain `.ts` logic tests in node.

```ts
import { sveltekit } from '@sveltejs/kit/vite';
import UnoCSS from 'unocss/vite';
import { defineConfig } from 'vitest/config';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// Single source of truth for the displayed version — `make bump` keeps
// package.json in sync with Cargo.toml, so the footer always matches the release.
const { version } = JSON.parse(
	readFileSync(fileURLToPath(new URL('./package.json', import.meta.url)), 'utf-8')
);

export default defineConfig({
	define: {
		__APP_VERSION__: JSON.stringify(version)
	},
	plugins: [UnoCSS(), sveltekit()],
	test: {
		projects: [
			{
				extends: './vite.config.ts',
				test: {
					name: 'client',
					browser: {
						enabled: true,
						provider: 'playwright',
						headless: true,
						instances: [{ browser: 'chromium' }]
					},
					include: ['src/**/*.svelte.{test,spec}.ts'],
					setupFiles: ['./vitest-setup-client.ts']
				}
			},
			{
				extends: './vite.config.ts',
				test: {
					name: 'server',
					environment: 'node',
					include: ['src/**/*.{test,spec}.ts'],
					exclude: ['src/**/*.svelte.{test,spec}.ts']
				}
			}
		]
	}
});
```

- [ ] **Step 2: Create the client setup file**

```ts
// site/vitest-setup-client.ts
// Runs before each client (browser) test file. `vitest-browser-svelte` +
// @vitest/browser provide `render`, locators, and `expect.element` matchers;
// no jest-dom import is needed in browser mode. Reserved for future globals.
export {};
```

- [ ] **Step 3: Add scripts to `site/package.json`**

Add to `"scripts"`:

```json
"test": "vitest run",
"test:watch": "vitest"
```

- [ ] **Step 4: Verify the config loads (no tests yet)**

```bash
cd site && bun run test
```

Expected: Vitest starts, reports "No test files found" (or runs 0 files) and exits 0. If it errors on the browser provider, re-run `bunx playwright install chromium`.

- [ ] **Step 5: Commit**

```bash
git add site/vite.config.ts site/vitest-setup-client.ts site/package.json
git commit -m "chore(site): configure vitest browser + server test projects"
```

### Task 0.3: Smoke test (green baseline)

**Files:**
- Create: `site/src/lib/components/ArrowIcon.svelte.test.ts`

`ArrowIcon.svelte` is a static SVG (no props) — the ideal smoke test to prove the harness renders a real component.

- [ ] **Step 1: Write the test**

```ts
// site/src/lib/components/ArrowIcon.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import ArrowIcon from './ArrowIcon.svelte';

test('renders an inline svg arrow', async () => {
	const screen = render(ArrowIcon);
	const svg = screen.container.querySelector('svg');
	expect(svg).not.toBeNull();
	expect(svg?.querySelector('path')).not.toBeNull();
});
```

- [ ] **Step 2: Run it**

```bash
cd site && bun run test -- src/lib/components/ArrowIcon.svelte.test.ts
```

Expected: PASS (1 test) in the `client` project.

- [ ] **Step 3: Commit**

```bash
git add site/src/lib/components/ArrowIcon.svelte.test.ts
git commit -m "test(site): smoke test for ArrowIcon (harness baseline)"
```

---

## Phase 1 — Tests for existing (unchanged) leaves

These leaves keep their names and markup; we only add coverage so the later refactors have a safety net. Snippet-child props are supplied with `createRawSnippet`.

### Task 1.1: `Eyebrow` + `Chip` tests

**Files:**
- Create: `site/src/lib/components/Eyebrow.svelte.test.ts`
- Create: `site/src/lib/components/Chip.svelte.test.ts`

- [ ] **Step 1: Write `Eyebrow.svelte.test.ts`**

```ts
// site/src/lib/components/Eyebrow.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { createRawSnippet } from 'svelte';
import { expect, test } from 'vitest';
import Eyebrow from './Eyebrow.svelte';

const label = createRawSnippet(() => ({ render: () => `<span>Routing engine</span>` }));

test('renders its child label and the dot marker', async () => {
	const screen = render(Eyebrow, { props: { children: label } });
	await expect.element(screen.getByText('Routing engine')).toBeInTheDocument();
	// The leading dot is a decorative span with the primary background.
	expect(screen.container.querySelector('span.bg-primary')).not.toBeNull();
});
```

- [ ] **Step 2: Write `Chip.svelte.test.ts`**

```ts
// site/src/lib/components/Chip.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import Chip from './Chip.svelte';

test('renders the label', async () => {
	const screen = render(Chip, { props: { label: 'tokio' } });
	await expect.element(screen.getByText('tokio')).toBeInTheDocument();
});

test('accent tone uses the accent-soft surface class', async () => {
	const screen = render(Chip, { props: { label: 'v0.4.8', tone: 'accent' } });
	const chip = screen.getByText('v0.4.8').element();
	expect(chip.className).toContain('bg-accent-soft');
});

test('default tone uses the paper-soft surface class', async () => {
	const screen = render(Chip, { props: { label: 'reqwest' } });
	const chip = screen.getByText('reqwest').element();
	expect(chip.className).toContain('bg-paper-soft');
});
```

- [ ] **Step 3: Run both**

```bash
cd site && bun run test -- src/lib/components/Eyebrow.svelte.test.ts src/lib/components/Chip.svelte.test.ts
```

Expected: PASS (4 tests).

- [ ] **Step 4: Commit**

```bash
git add site/src/lib/components/Eyebrow.svelte.test.ts site/src/lib/components/Chip.svelte.test.ts
git commit -m "test(site): cover Eyebrow and Chip leaves"
```

### Task 1.2: `SectionHead` + `CrateCard` + `BrandMark` tests

**Files:**
- Create: `site/src/lib/components/SectionHead.svelte.test.ts`
- Create: `site/src/lib/components/CrateCard.svelte.test.ts`
- Create: `site/src/lib/components/BrandMark.svelte.test.ts`

- [ ] **Step 1: Write `SectionHead.svelte.test.ts`**

```ts
// site/src/lib/components/SectionHead.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import SectionHead from './SectionHead.svelte';

test('renders eyebrow and title, omits lede when empty', async () => {
	const screen = render(SectionHead, {
		props: { eyebrow: 'The routing engine', title: 'Everything a request needs.' }
	});
	await expect.element(screen.getByText('The routing engine')).toBeInTheDocument();
	await expect.element(screen.getByRole('heading', { level: 2 })).toBeInTheDocument();
	expect(screen.container.querySelector('p')).toBeNull();
});

test('renders the lede paragraph when provided', async () => {
	const screen = render(SectionHead, {
		props: { eyebrow: 'E', title: 'T', lede: 'No database of its own.' }
	});
	await expect.element(screen.getByText('No database of its own.')).toBeInTheDocument();
});

test('center align adds the centering classes', async () => {
	const screen = render(SectionHead, {
		props: { eyebrow: 'E', title: 'T', align: 'center' }
	});
	expect(screen.container.querySelector('.text-center')).not.toBeNull();
});
```

- [ ] **Step 2: Write `CrateCard.svelte.test.ts`**

```ts
// site/src/lib/components/CrateCard.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import CrateCard from './CrateCard.svelte';

const crate = {
	name: 'local-providers',
	version: 'v0.4.8',
	body: 'In-process inference adapters.',
	chips: ['llama-cpp', 'fastembed', 'ort'],
	note: 'feature-gated'
};

test('renders name, version chip, body, dep chips and note', async () => {
	const screen = render(CrateCard, { props: { crate } });
	await expect.element(screen.getByText('local-providers')).toBeInTheDocument();
	await expect.element(screen.getByText('v0.4.8')).toBeInTheDocument();
	await expect.element(screen.getByText('In-process inference adapters.')).toBeInTheDocument();
	await expect.element(screen.getByText('llama-cpp')).toBeInTheDocument();
	await expect.element(screen.getByText('feature-gated')).toBeInTheDocument();
});

test('omits the note span when the crate has no note', async () => {
	const screen = render(CrateCard, { props: { crate: { ...crate, note: undefined } } });
	expect(screen.container.textContent).not.toContain('feature-gated');
});
```

- [ ] **Step 3: Write `BrandMark.svelte.test.ts`**

```ts
// site/src/lib/components/BrandMark.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import BrandMark from './BrandMark.svelte';

test('on-blue variant renders the rounded accent square', async () => {
	const screen = render(BrandMark, { props: { variant: 'on-blue' } });
	const svg = screen.container.querySelector('svg');
	expect(svg?.querySelector('rect')).not.toBeNull();
	expect(svg?.getAttribute('aria-hidden')).toBe('true');
});

test('white variant renders the transparent glyph (no square)', async () => {
	const screen = render(BrandMark, { props: { variant: 'white' } });
	const svg = screen.container.querySelector('svg');
	expect(svg?.querySelector('rect')).toBeNull();
	expect(svg?.querySelectorAll('path').length).toBeGreaterThan(0);
});

test('accepts a custom class', async () => {
	const screen = render(BrandMark, { props: { class: 'h-7 w-7' } });
	expect(screen.container.querySelector('svg')?.getAttribute('class')).toContain('h-7');
});
```

- [ ] **Step 4: Run all three**

```bash
cd site && bun run test -- src/lib/components/SectionHead.svelte.test.ts src/lib/components/CrateCard.svelte.test.ts src/lib/components/BrandMark.svelte.test.ts
```

Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/SectionHead.svelte.test.ts site/src/lib/components/CrateCard.svelte.test.ts site/src/lib/components/BrandMark.svelte.test.ts
git commit -m "test(site): cover SectionHead, CrateCard, BrandMark leaves"
```

### Task 1.3: `Seo` test (with `$app/state` mocked)

**Files:**
- Create: `site/src/lib/components/Seo.svelte.test.ts`

- [ ] **Step 1: Write the test**

`Seo` writes into `<svelte:head>` and derives the canonical URL from `$app/state`'s `page.url.pathname`; mock that module.

```ts
// site/src/lib/components/Seo.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';

vi.mock('$app/state', () => ({
	page: { url: new URL('https://gateway.sensei-hq.com/') }
}));

import Seo from './Seo.svelte';

test('sets document title, description and canonical meta', async () => {
	render(Seo, { props: { title: 'gateway — routing', description: 'Provider-agnostic.' } });
	expect(document.title).toBe('gateway — routing');
	const desc = document.head.querySelector('meta[name="description"]');
	expect(desc?.getAttribute('content')).toBe('Provider-agnostic.');
	const canonical = document.head.querySelector('link[rel="canonical"]');
	expect(canonical?.getAttribute('href')).toContain('gateway.sensei-hq.com');
});

test('adds a noindex robots meta when noindex is set', async () => {
	render(Seo, {
		props: { title: 'T', description: 'D', noindex: true }
	});
	const robots = document.head.querySelector('meta[name="robots"]');
	expect(robots?.getAttribute('content')).toBe('noindex');
});
```

- [ ] **Step 2: Run it**

```bash
cd site && bun run test -- src/lib/components/Seo.svelte.test.ts
```

Expected: PASS (2 tests). If `$lib/seo` imports fail, confirm the mock path is exactly `$app/state`.

- [ ] **Step 3: Commit**

```bash
git add site/src/lib/components/Seo.svelte.test.ts
git commit -m "test(site): cover Seo head/canonical output"
```

---

## Phase 2 — Leaf consolidation (rename + merge)

### Task 2.1: Rename `FeatureCard` → `InfoCard`

**Files:**
- Create: `site/src/lib/components/InfoCard.svelte` (moved from `FeatureCard.svelte`)
- Delete: `site/src/lib/components/FeatureCard.svelte`
- Create: `site/src/lib/components/InfoCard.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte` (import + usage — temporary; the block extraction in Phase 3 supersedes this)

- [ ] **Step 1: Create `InfoCard.svelte` with the current `FeatureCard` markup verbatim**

```svelte
<!-- site/src/lib/components/InfoCard.svelte -->
<script lang="ts">
	let { tag, title, body }: { tag: string; title: string; body: string } = $props();
</script>

<div
	class="group flex flex-col gap-3 rounded-xl border border-paper-edge bg-paper-mute p-6 transition-colors hover:border-accent"
>
	<span class="font-mono text-xs text-ink-soft transition-colors group-hover:text-primary">{tag}</span>
	<h3 class="font-display font-semibold text-xl text-ink">{title}</h3>
	<p class="text-sm text-ink-mute text-pretty">{body}</p>
</div>
```

- [ ] **Step 2: Delete the old file**

```bash
git rm site/src/lib/components/FeatureCard.svelte
```

- [ ] **Step 3: Update the import + tag in `+page.svelte`**

In `site/src/routes/+page.svelte`, change the import line
`import FeatureCard from '$lib/components/FeatureCard.svelte';`
to
`import InfoCard from '$lib/components/InfoCard.svelte';`
and change the usage `<FeatureCard tag={f.tag} title={f.title} body={f.body} />` to
`<InfoCard tag={f.tag} title={f.title} body={f.body} />`.

- [ ] **Step 4: Write `InfoCard.svelte.test.ts` (includes a computed-style border check)**

```ts
// site/src/lib/components/InfoCard.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import InfoCard from './InfoCard.svelte';

test('renders tag, title and body', async () => {
	const screen = render(InfoCard, {
		props: { tag: 'fallback', title: 'Named fallback chains', body: 'Chain endpoints by name.' }
	});
	await expect.element(screen.getByText('fallback')).toBeInTheDocument();
	await expect.element(screen.getByRole('heading', { level: 3 })).toBeInTheDocument();
	await expect.element(screen.getByText('Chain endpoints by name.')).toBeInTheDocument();
});

test('card border computes to a real 1px solid line (not collapsed)', async () => {
	const screen = render(InfoCard, { props: { tag: 't', title: 'T', body: 'B' } });
	const card = screen.container.querySelector('div')!;
	const cs = getComputedStyle(card);
	expect(cs.borderTopStyle).toBe('solid');
	expect(parseFloat(cs.borderTopWidth)).toBeCloseTo(1, 1);
	expect(cs.boxSizing).toBe('border-box');
});
```

- [ ] **Step 5: Run tests + type check**

```bash
cd site && bun run test -- src/lib/components/InfoCard.svelte.test.ts && bun run check
```

Expected: tests PASS (2), `svelte-check` reports 0 errors (no dangling `FeatureCard` references).

- [ ] **Step 6: Commit**

```bash
git add site/src/lib/components/InfoCard.svelte site/src/lib/components/InfoCard.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): rename FeatureCard to InfoCard (mockup 1:1) + tests"
```

### Task 2.2: Merge `CodeWindow` + `UsageTabs` → `CodeFrame`

**Files:**
- Create: `site/src/lib/components/CodeFrame.svelte`
- Delete: `site/src/lib/components/CodeWindow.svelte`, `site/src/lib/components/UsageTabs.svelte`
- Create: `site/src/lib/components/CodeFrame.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte` (hero uses `CodeFrame`), and the usage section (temporary until Phase 3 extracts `UsageSection`)

- [ ] **Step 1: Create `CodeFrame.svelte`**

One leaf with both modes. No `tabs` → traffic-light chrome + filename + copy (old `CodeWindow`). With `tabs` → tablist bar (old `UsageTabs`), controllable via `activeTab`/`onSelectTab`, uncontrolled by default. Reuses the app's `.code-surface` `--code-*` tokens.

```svelte
<!-- site/src/lib/components/CodeFrame.svelte -->
<script lang="ts">
	type Tab = { id: string; label: string; code: string };
	let {
		filename = 'code',
		code = '',
		copyable = true,
		tabs,
		activeTab,
		onSelectTab
	}: {
		filename?: string;
		code?: string;
		copyable?: boolean;
		tabs?: Tab[];
		activeTab?: string;
		onSelectTab?: (id: string) => void;
	} = $props();

	let internalTab = $state<string | null>(null);
	const hasTabs = $derived(!!tabs && tabs.length > 0);
	const activeId = $derived(activeTab ?? internalTab ?? tabs?.[0]?.id);
	const current = $derived(tabs?.find((t) => t.id === activeId) ?? tabs?.[0]);
	const shownCode = $derived(hasTabs ? (current?.code ?? '') : code);

	let copied = $state(false);
	async function copy() {
		try {
			await navigator.clipboard.writeText(shownCode);
			copied = true;
			setTimeout(() => (copied = false), 1500);
		} catch {
			/* clipboard unavailable — no-op */
		}
	}

	function selectTab(id: string) {
		internalTab = id;
		onSelectTab?.(id);
	}
</script>

<div
	class="code-surface overflow-hidden"
	style="border-radius:14px; border:1px solid var(--code-border); box-shadow: var(--code-shadow)"
>
	{#if hasTabs}
		<div
			class="flex items-center"
			style="background: var(--code-bar); gap:2px; padding:8px 10px 0; border-bottom:1px solid var(--code-border)"
			role="tablist"
		>
			{#each tabs! as t (t.id)}
				<button
					role="tab"
					aria-selected={t.id === activeId}
					onclick={() => selectTab(t.id)}
					class="font-mono"
					style="appearance:none; border:none; cursor:pointer; font-size:13px; font-weight:500; padding:9px 14px; border-radius:8px 8px 0 0; {t.id ===
					activeId
						? 'color: var(--code-text); background: var(--code-bg);'
						: 'color: var(--code-idle); background: transparent;'}"
				>
					{t.label}
				</button>
			{/each}
		</div>
	{:else}
		<div
			class="flex items-center gap-2"
			style="background: var(--code-bar); padding:11px 14px; border-bottom:1px solid var(--code-border)"
		>
			<span style="width:11px;height:11px;border-radius:50%;background:#ff5f57"></span>
			<span style="width:11px;height:11px;border-radius:50%;background:#febc2e"></span>
			<span style="width:11px;height:11px;border-radius:50%;background:#28c840"></span>
			<span class="font-mono" style="font-size:12.5px; color: var(--code-idle); margin-left:6px"
				>{filename}</span
			>
			{#if copyable}
				<button
					onclick={copy}
					class="font-mono"
					style="margin-left:auto; appearance:none; border:none; cursor:pointer; font-size:11.5px; color: var(--code-copy-text); background: var(--code-copy-bg); padding:4px 10px; border-radius:6px"
					aria-label="Copy code to clipboard"
				>
					{copied ? 'Copied' : 'Copy'}
				</button>
			{/if}
		</div>
	{/if}
	<pre
		class="font-mono"
		style="margin:0; background: var(--code-bg); color: var(--code-text); padding:20px; font-size:13.5px; line-height:1.8; overflow-x:auto; min-height:150px">{shownCode}</pre>
</div>
```

- [ ] **Step 2: Delete the two old components**

```bash
git rm site/src/lib/components/CodeWindow.svelte site/src/lib/components/UsageTabs.svelte
```

- [ ] **Step 3: Update `+page.svelte` call sites**

In `site/src/routes/+page.svelte`:
- Replace `import CodeWindow from '$lib/components/CodeWindow.svelte';` and `import UsageTabs from '$lib/components/UsageTabs.svelte';` with a single `import CodeFrame from '$lib/components/CodeFrame.svelte';`.
- Hero: change `<CodeWindow filename={hero.code.filename} code={hero.code.source} />` to `<CodeFrame filename={hero.code.filename} code={hero.code.source} />`.
- Usage: change `<UsageTabs tabs={usage.tabs} />` to `<CodeFrame tabs={usage.tabs} />`.

- [ ] **Step 4: Write `CodeFrame.svelte.test.ts`**

```ts
// site/src/lib/components/CodeFrame.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi, beforeEach } from 'vitest';
import CodeFrame from './CodeFrame.svelte';

beforeEach(() => {
	vi.restoreAllMocks();
});

test('chrome mode: shows filename, copy button and code', async () => {
	const screen = render(CodeFrame, {
		props: { filename: 'Cargo.toml', code: '[dependencies]' }
	});
	await expect.element(screen.getByText('Cargo.toml')).toBeInTheDocument();
	await expect.element(screen.getByText('[dependencies]')).toBeInTheDocument();
	await expect.element(screen.getByRole('button', { name: /copy/i })).toBeInTheDocument();
	// No tablist in chrome mode.
	expect(screen.container.querySelector('[role="tablist"]')).toBeNull();
});

test('chrome mode: copy writes the code to the clipboard and flips the label', async () => {
	const writeText = vi.fn().mockResolvedValue(undefined);
	Object.assign(navigator, { clipboard: { writeText } });
	const screen = render(CodeFrame, { props: { filename: 'f', code: 'cargo build' } });
	await screen.getByRole('button', { name: /copy/i }).click();
	expect(writeText).toHaveBeenCalledWith('cargo build');
	await expect.element(screen.getByText('Copied')).toBeInTheDocument();
});

test('tabs mode: renders a tablist and switches active code on click', async () => {
	const tabs = [
		{ id: 'add', label: 'Cargo.toml', code: 'ADD_CODE' },
		{ id: 'patch', label: 'Local dev', code: 'PATCH_CODE' }
	];
	const screen = render(CodeFrame, { props: { tabs } });
	await expect.element(screen.getByRole('tablist')).toBeInTheDocument();
	await expect.element(screen.getByText('ADD_CODE')).toBeInTheDocument();
	await screen.getByRole('tab', { name: 'Local dev' }).click();
	await expect.element(screen.getByText('PATCH_CODE')).toBeInTheDocument();
});

test('tabs mode: aria-selected tracks the active tab', async () => {
	const tabs = [
		{ id: 'a', label: 'A', code: 'x' },
		{ id: 'b', label: 'B', code: 'y' }
	];
	const screen = render(CodeFrame, { props: { tabs, activeTab: 'b' } });
	const tabB = screen.getByRole('tab', { name: 'B' }).element();
	expect(tabB.getAttribute('aria-selected')).toBe('true');
});
```

- [ ] **Step 5: Run tests + type check**

```bash
cd site && bun run test -- src/lib/components/CodeFrame.svelte.test.ts && bun run check
```

Expected: tests PASS (4), `svelte-check` 0 errors.

- [ ] **Step 6: Commit**

```bash
git add site/src/lib/components/CodeFrame.svelte site/src/lib/components/CodeFrame.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): merge CodeWindow+UsageTabs into CodeFrame + tests"
```

---

## Phase 3 — Section block extraction + `+page.svelte` slim-down

Each task extracts one segment's markup **verbatim** from the current `+page.svelte` into its own block that reads its `data.ts` slice, adds a test, and removes that markup from `+page.svelte`. Do them in order; after the last, `+page.svelte` is composition-only.

> Reference: the pre-extraction `+page.svelte` markup for each segment is reproduced in each task's Step 1 so you don't need to read tasks out of order.

### Task 3.1: `HeroSection`

**Files:**
- Create: `site/src/lib/components/HeroSection.svelte`
- Create: `site/src/lib/components/HeroSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `HeroSection.svelte` (reads the `hero` slice; uses `CodeFrame`, `ArrowIcon`, Rokkit `Button`)**

```svelte
<!-- site/src/lib/components/HeroSection.svelte -->
<script lang="ts">
	import { Button } from '@rokkit/ui';
	import CodeFrame from './CodeFrame.svelte';
	import ArrowIcon from './ArrowIcon.svelte';
	import { hero } from '$lib/data';
</script>

<section class="relative overflow-hidden">
	<div class="pointer-events-none absolute inset-0 bg-grid mask-fade-b opacity-60"></div>
	<div
		class="anim-rise relative mx-auto flex max-w-content flex-col items-center gap-6 px-6 pb-16 pt-20 text-center md:pt-28"
	>
		<div
			class="inline-flex items-center gap-2 rounded-full border border-accent-line bg-accent-soft px-3 py-1 text-sm font-semibold text-primary"
		>
			<span class="font-mono">{hero.badge[0]}</span>
			<span class="text-ink-soft">·</span>
			{hero.badge[1]}
		</div>
		<h1 class="max-w-3xl font-display font-semibold text-display text-ink text-balance">
			{hero.title}
		</h1>
		<p class="max-w-2xl text-lg text-ink-mute text-pretty">{hero.lede}</p>
		<div class="flex flex-wrap items-center justify-center gap-3 pt-1">
			<Button href={hero.primaryCta.href} variant="primary" size="lg">
				{hero.primaryCta.label}
				<ArrowIcon />
			</Button>
			<Button href={hero.secondaryCta.href} variant="default" style="outline" size="lg">
				{hero.secondaryCta.label}
			</Button>
		</div>
		<div class="mt-6 w-full max-w-2xl text-left">
			<CodeFrame filename={hero.code.filename} code={hero.code.source} />
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `HeroSection.svelte.test.ts`**

```ts
// site/src/lib/components/HeroSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import HeroSection from './HeroSection.svelte';
import { hero } from '$lib/data';

test('renders the hero title, lede and both CTAs from data', async () => {
	const screen = render(HeroSection);
	await expect.element(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
	await expect.element(screen.getByText(hero.title)).toBeInTheDocument();
	await expect.element(screen.getByText(hero.lede)).toBeInTheDocument();
	await expect.element(screen.getByText(hero.primaryCta.label)).toBeInTheDocument();
	await expect.element(screen.getByText(hero.secondaryCta.label)).toBeInTheDocument();
});

test('embeds the install CodeFrame with the Cargo.toml filename', async () => {
	const screen = render(HeroSection);
	await expect.element(screen.getByText(hero.code.filename)).toBeInTheDocument();
});
```

- [ ] **Step 3: Replace the hero block in `+page.svelte`**

Remove the entire `<!-- HERO -->` `<section>…</section>` block (the one starting `<section class="relative overflow-hidden">`), add `import HeroSection from '$lib/components/HeroSection.svelte';`, and render `<HeroSection />` where the block was. Also remove the now-unused `hero` import from the `$lib/data` import list if nothing else in `+page.svelte` uses it yet (leave other slice imports intact).

- [ ] **Step 4: Run tests + type check**

```bash
cd site && bun run test -- src/lib/components/HeroSection.svelte.test.ts && bun run check
```

Expected: PASS (2), 0 type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/HeroSection.svelte site/src/lib/components/HeroSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract HeroSection block + test"
```

### Task 3.2: `ProofStrip`

**Files:**
- Create: `site/src/lib/components/ProofStrip.svelte`
- Create: `site/src/lib/components/ProofStrip.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `ProofStrip.svelte`**

```svelte
<!-- site/src/lib/components/ProofStrip.svelte -->
<script lang="ts">
	import { proof } from '$lib/data';
</script>

<div class="border-y border-paper-edge bg-paper-soft">
	<div
		class="mx-auto flex max-w-content flex-wrap items-center gap-x-6 gap-y-2 px-6 py-4 font-mono text-sm text-ink-soft"
	>
		<span class="font-medium text-ink-mute">{proof.label}</span>
		{#each proof.providers as p (p)}
			<span>{p}</span>
		{/each}
	</div>
</div>
```

- [ ] **Step 2: Write `ProofStrip.svelte.test.ts`**

```ts
// site/src/lib/components/ProofStrip.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import ProofStrip from './ProofStrip.svelte';
import { proof } from '$lib/data';

test('renders the label and every provider', async () => {
	const screen = render(ProofStrip);
	await expect.element(screen.getByText(proof.label)).toBeInTheDocument();
	for (const p of proof.providers) {
		await expect.element(screen.getByText(p)).toBeInTheDocument();
	}
});
```

- [ ] **Step 3: Replace the proof block in `+page.svelte`**

Remove the `<!-- PROOF STRIP -->` `<div class="border-y …">…</div>` block, add `import ProofStrip from '$lib/components/ProofStrip.svelte';`, render `<ProofStrip />`, and drop `proof` from the data import.

- [ ] **Step 4: Run + check**

```bash
cd site && bun run test -- src/lib/components/ProofStrip.svelte.test.ts && bun run check
```

Expected: PASS (1), 0 type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/ProofStrip.svelte site/src/lib/components/ProofStrip.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract ProofStrip block + test"
```

### Task 3.3: `FeaturesSection`

**Files:**
- Create: `site/src/lib/components/FeaturesSection.svelte`
- Create: `site/src/lib/components/FeaturesSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `FeaturesSection.svelte` (uses `SectionHead` + `InfoCard`)**

```svelte
<!-- site/src/lib/components/FeaturesSection.svelte -->
<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import InfoCard from './InfoCard.svelte';
	import { features } from '$lib/data';
</script>

<section id="features" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={features.eyebrow} title={features.title} lede={features.lede} />
		<div class="mt-12 grid gap-4 sm:grid-cols-2 md:grid-cols-3">
			{#each features.items as f (f.tag)}
				<InfoCard tag={f.tag} title={f.title} body={f.body} />
			{/each}
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `FeaturesSection.svelte.test.ts`**

```ts
// site/src/lib/components/FeaturesSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import FeaturesSection from './FeaturesSection.svelte';
import { features } from '$lib/data';

test('renders the section heading and one InfoCard per feature', async () => {
	const screen = render(FeaturesSection);
	await expect.element(screen.getByText(features.title)).toBeInTheDocument();
	for (const f of features.items) {
		await expect.element(screen.getByText(f.title)).toBeInTheDocument();
	}
	// One h3 heading per feature card.
	expect(screen.container.querySelectorAll('h3').length).toBe(features.items.length);
});
```

- [ ] **Step 3: Replace the features block in `+page.svelte`**

Remove the `<!-- FEATURES -->` `<section id="features" …>…</section>`, add `import FeaturesSection from '$lib/components/FeaturesSection.svelte';`, render `<FeaturesSection />`, and drop the now-unused `features` import and the `InfoCard` import (moved into the block).

- [ ] **Step 4: Run + check**

```bash
cd site && bun run test -- src/lib/components/FeaturesSection.svelte.test.ts && bun run check
```

Expected: PASS (1), 0 type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/FeaturesSection.svelte site/src/lib/components/FeaturesSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract FeaturesSection block + test"
```

### Task 3.4: `CratesSection`

**Files:**
- Create: `site/src/lib/components/CratesSection.svelte`
- Create: `site/src/lib/components/CratesSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `CratesSection.svelte` (uses `SectionHead` + `CrateCard`)**

```svelte
<!-- site/src/lib/components/CratesSection.svelte -->
<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import CrateCard from './CrateCard.svelte';
	import { crates } from '$lib/data';
</script>

<section id="crates" class="grid-section border-y border-paper-edge bg-paper-soft">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={crates.eyebrow} title={crates.title} />
		<div class="mt-12 grid gap-6 md:grid-cols-2">
			{#each crates.items as c (c.name)}
				<CrateCard crate={c} />
			{/each}
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `CratesSection.svelte.test.ts`**

```ts
// site/src/lib/components/CratesSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import CratesSection from './CratesSection.svelte';
import { crates } from '$lib/data';

test('renders the heading and one card per crate', async () => {
	const screen = render(CratesSection);
	await expect.element(screen.getByText(crates.title)).toBeInTheDocument();
	for (const c of crates.items) {
		await expect.element(screen.getByText(c.name)).toBeInTheDocument();
	}
});
```

- [ ] **Step 3: Replace the crates block in `+page.svelte`**

Remove the `<!-- CRATES -->` `<section id="crates" …>…</section>`, add `import CratesSection from '$lib/components/CratesSection.svelte';`, render `<CratesSection />`, drop the `crates`, `CrateCard`, and `SectionHead` imports if unused elsewhere in `+page.svelte`.

- [ ] **Step 4: Run + check**

```bash
cd site && bun run test -- src/lib/components/CratesSection.svelte.test.ts && bun run check
```

Expected: PASS (1), 0 type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/CratesSection.svelte site/src/lib/components/CratesSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract CratesSection block + test"
```

### Task 3.5: `UsageSection`

**Files:**
- Create: `site/src/lib/components/UsageSection.svelte`
- Create: `site/src/lib/components/UsageSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `UsageSection.svelte` (uses `SectionHead` + `CodeFrame` tabs)**

```svelte
<!-- site/src/lib/components/UsageSection.svelte -->
<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import CodeFrame from './CodeFrame.svelte';
	import { usage } from '$lib/data';
</script>

<section id="usage" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={usage.eyebrow} title={usage.title} lede={usage.lede} />
		<div class="mt-10">
			<CodeFrame tabs={usage.tabs} />
			<p class="mt-4 px-1 font-mono text-sm text-ink-soft">{usage.note}</p>
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `UsageSection.svelte.test.ts`**

```ts
// site/src/lib/components/UsageSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import UsageSection from './UsageSection.svelte';
import { usage } from '$lib/data';

test('renders the heading, a tablist with every usage tab, and the note', async () => {
	const screen = render(UsageSection);
	await expect.element(screen.getByText(usage.title)).toBeInTheDocument();
	await expect.element(screen.getByRole('tablist')).toBeInTheDocument();
	for (const t of usage.tabs) {
		await expect.element(screen.getByRole('tab', { name: t.label })).toBeInTheDocument();
	}
	await expect.element(screen.getByText(usage.note)).toBeInTheDocument();
});
```

- [ ] **Step 3: Replace the usage block in `+page.svelte`**

Remove the `<!-- USAGE -->` `<section id="usage" …>…</section>`, add `import UsageSection from '$lib/components/UsageSection.svelte';`, render `<UsageSection />`, drop the `usage` and `CodeFrame` imports if now unused in `+page.svelte`.

- [ ] **Step 4: Run + check**

```bash
cd site && bun run test -- src/lib/components/UsageSection.svelte.test.ts && bun run check
```

Expected: PASS (1), 0 type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/UsageSection.svelte site/src/lib/components/UsageSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract UsageSection block + test"
```

### Task 3.6: `ConsumersSection`

**Files:**
- Create: `site/src/lib/components/ConsumersSection.svelte`
- Create: `site/src/lib/components/ConsumersSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `ConsumersSection.svelte` (inlined consumer card markup, verbatim from `+page.svelte`)**

```svelte
<!-- site/src/lib/components/ConsumersSection.svelte -->
<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import { consumers } from '$lib/data';
</script>

<section id="consumers" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={consumers.eyebrow} title={consumers.title} lede={consumers.lede} />
		<div class="mt-12 grid gap-6 sm:grid-cols-2">
			{#each consumers.items as c (c.name)}
				<a
					href={c.repo}
					class="flex items-center gap-4 rounded-xl border border-paper-edge bg-paper-mute p-7 transition-colors hover:border-accent"
				>
					<span
						class="grid h-11 w-11 shrink-0 place-items-center rounded-lg border border-accent-line bg-accent-soft font-mono text-xl font-semibold text-primary"
					>
						{c.glyph}
					</span>
					<div>
						<div class="font-display font-semibold text-lg text-ink">{c.name}</div>
						<div class="font-mono text-sm text-ink-soft">
							{c.repo.replace('https://', '')}
						</div>
					</div>
				</a>
			{/each}
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `ConsumersSection.svelte.test.ts`**

```ts
// site/src/lib/components/ConsumersSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import ConsumersSection from './ConsumersSection.svelte';
import { consumers } from '$lib/data';

test('renders a linked card per consumer with repo href', async () => {
	const screen = render(ConsumersSection);
	await expect.element(screen.getByText(consumers.title)).toBeInTheDocument();
	for (const c of consumers.items) {
		const link = screen.getByRole('link', { name: new RegExp(c.name) }).element();
		expect(link.getAttribute('href')).toBe(c.repo);
	}
});
```

- [ ] **Step 3: Replace the consumers block in `+page.svelte`**

Remove the `<!-- CONSUMERS -->` `<section id="consumers" …>…</section>`, add `import ConsumersSection from '$lib/components/ConsumersSection.svelte';`, render `<ConsumersSection />`, drop the `consumers` import.

- [ ] **Step 4: Run + check**

```bash
cd site && bun run test -- src/lib/components/ConsumersSection.svelte.test.ts && bun run check
```

Expected: PASS (1), 0 type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/ConsumersSection.svelte site/src/lib/components/ConsumersSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract ConsumersSection block + test"
```

### Task 3.7: `VersioningSection`

**Files:**
- Create: `site/src/lib/components/VersioningSection.svelte`
- Create: `site/src/lib/components/VersioningSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

Note: the current markup computes an inverted mode via `vibe`. Move that logic into the block.

- [ ] **Step 1: Create `VersioningSection.svelte`**

```svelte
<!-- site/src/lib/components/VersioningSection.svelte -->
<script lang="ts">
	import { vibe } from '@rokkit/states';
	import Eyebrow from './Eyebrow.svelte';
	import { versioning } from '$lib/data';

	// The versioning panel flips to the opposite mode of the page (mockup detail).
	const invMode = $derived(vibe.mode === 'dark' ? 'light' : 'dark');
</script>

<section
	id="versioning"
	data-mode={invMode}
	class="grid-section border-y border-paper-edge bg-paper"
>
	<div class="mx-auto grid max-w-content items-center gap-10 px-6 py-section md:grid-cols-2 md:gap-14">
		<div class="flex flex-col gap-4">
			<Eyebrow>{versioning.eyebrow}</Eyebrow>
			<h2 class="font-display font-semibold text-h2 text-ink text-balance">{versioning.title}</h2>
			<p class="max-w-xl text-lg text-ink-mute text-pretty">{versioning.lede}</p>
		</div>
		<div class="flex flex-col gap-3">
			{#each versioning.steps as s (s.n)}
				<div class="flex items-start gap-3 rounded-lg border border-paper-edge bg-paper-mute px-5 py-4">
					<span class="font-mono text-sm text-primary">{s.n}</span>
					<div>
						<div class="font-display font-semibold text-ink">{s.title}</div>
						<div class="mt-1 font-mono text-sm text-ink-soft">{s.note}</div>
					</div>
				</div>
			{/each}
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `VersioningSection.svelte.test.ts`**

```ts
// site/src/lib/components/VersioningSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import VersioningSection from './VersioningSection.svelte';
import { versioning } from '$lib/data';

test('renders the title and every versioning step', async () => {
	const screen = render(VersioningSection);
	await expect.element(screen.getByText(versioning.title)).toBeInTheDocument();
	for (const s of versioning.steps) {
		await expect.element(screen.getByText(s.title)).toBeInTheDocument();
		await expect.element(screen.getByText(s.n)).toBeInTheDocument();
	}
});

test('sets a data-mode attribute on the section (inverted panel)', async () => {
	const screen = render(VersioningSection);
	const section = screen.container.querySelector('#versioning')!;
	expect(['light', 'dark']).toContain(section.getAttribute('data-mode'));
});
```

- [ ] **Step 3: Replace the versioning block in `+page.svelte`**

Remove the `<!-- VERSIONING (inverted panel) -->` `<section id="versioning" …>…</section>`, add `import VersioningSection from '$lib/components/VersioningSection.svelte';`, render `<VersioningSection />`, and remove the now-unused `versioning` import, the `Eyebrow` import, the `vibe` import, and the `invMode` `$derived` line from `+page.svelte`.

- [ ] **Step 4: Run + check**

```bash
cd site && bun run test -- src/lib/components/VersioningSection.svelte.test.ts && bun run check
```

Expected: PASS (2), 0 type errors.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/VersioningSection.svelte site/src/lib/components/VersioningSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract VersioningSection block + test"
```

### Task 3.8: `CtaSection` + finalize composition-only `+page.svelte`

**Files:**
- Create: `site/src/lib/components/CtaSection.svelte`
- Create: `site/src/lib/components/CtaSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `CtaSection.svelte`**

```svelte
<!-- site/src/lib/components/CtaSection.svelte -->
<script lang="ts">
	import { Button } from '@rokkit/ui';
	import ArrowIcon from './ArrowIcon.svelte';
	import { start } from '$lib/data';
</script>

<section id="start" class="grid-section">
	<div class="mx-auto flex max-w-content flex-col items-center gap-5 px-6 py-section text-center">
		<h2 class="max-w-2xl font-display font-semibold text-h2 text-ink text-balance">{start.title}</h2>
		<p class="max-w-xl text-lg text-ink-mute text-pretty">{start.lede}</p>
		<div class="flex flex-wrap items-center justify-center gap-3 pt-2">
			<Button href={start.primaryCta.href} variant="primary" size="lg">
				{start.primaryCta.label}
				<ArrowIcon />
			</Button>
			<Button href={start.secondaryCta.href} variant="default" style="outline" size="lg">
				{start.secondaryCta.label}
			</Button>
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `CtaSection.svelte.test.ts`**

```ts
// site/src/lib/components/CtaSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import CtaSection from './CtaSection.svelte';
import { start } from '$lib/data';

test('renders the CTA title, lede and both action buttons', async () => {
	const screen = render(CtaSection);
	await expect.element(screen.getByText(start.title)).toBeInTheDocument();
	await expect.element(screen.getByText(start.lede)).toBeInTheDocument();
	await expect.element(screen.getByText(start.primaryCta.label)).toBeInTheDocument();
	await expect.element(screen.getByText(start.secondaryCta.label)).toBeInTheDocument();
});
```

- [ ] **Step 3: Rewrite `+page.svelte` to (near) composition-only**

After removing the CTA block, `+page.svelte` is composition-only **except** the architecture segment, which stays inline here (using the current simple `ArchDiagram`) and is extracted in Phase 4 — this keeps the file compiling with no broken intermediate. Write exactly:

```svelte
<script lang="ts">
	import Seo from '$lib/components/Seo.svelte';
	import SectionHead from '$lib/components/SectionHead.svelte';
	import ArchDiagram from '$lib/components/ArchDiagram.svelte';
	import HeroSection from '$lib/components/HeroSection.svelte';
	import ProofStrip from '$lib/components/ProofStrip.svelte';
	import FeaturesSection from '$lib/components/FeaturesSection.svelte';
	import CratesSection from '$lib/components/CratesSection.svelte';
	import UsageSection from '$lib/components/UsageSection.svelte';
	import ConsumersSection from '$lib/components/ConsumersSection.svelte';
	import VersioningSection from '$lib/components/VersioningSection.svelte';
	import CtaSection from '$lib/components/CtaSection.svelte';
	import { architecture } from '$lib/data';

	const description =
		'gateway is a provider-agnostic LLM inference routing engine for Rust — fallback chains, per-endpoint circuit breaker, budget management and request tracing across ~16 cloud providers plus in-process local models, behind one trait-based config.';
</script>

<Seo title="gateway — LLM inference routing engine for Rust" {description} />

<span id="top"></span>

<HeroSection />
<ProofStrip />
<FeaturesSection />
<CratesSection />
<UsageSection />

<!-- ARCHITECTURE — extracted into ArchitectureSection in Task 4.3 -->
<section id="architecture" class="grid-section border-y border-paper-edge bg-paper-soft">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={architecture.eyebrow} title={architecture.title} />
		<div class="mt-12">
			<ArchDiagram />
			<p class="mt-6 text-center font-mono text-xs text-ink-soft">{architecture.caption}</p>
		</div>
	</div>
</section>

<ConsumersSection />
<VersioningSection />
<CtaSection />
```

- [ ] **Step 4: Run + check + full suite**

```bash
cd site && bun run test -- src/lib/components/CtaSection.svelte.test.ts && bun run check && bun run test
```

Expected: CtaSection PASS (1), 0 type errors, full suite green.

- [ ] **Step 5: Commit**

```bash
git add site/src/lib/components/CtaSection.svelte site/src/lib/components/CtaSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "refactor(site): extract CtaSection + make +page.svelte composition-only"
```

### Task 3.9: Rename `Nav` → `SiteHeader`, `Footer` → `SiteFooter`

**Files:**
- Create: `site/src/lib/components/SiteHeader.svelte` (from `Nav.svelte`)
- Create: `site/src/lib/components/SiteFooter.svelte` (from `Footer.svelte`)
- Delete: `site/src/lib/components/Nav.svelte`, `site/src/lib/components/Footer.svelte`
- Create: `site/src/lib/components/SiteHeader.svelte.test.ts`, `site/src/lib/components/SiteFooter.svelte.test.ts`
- Modify: `site/src/routes/+layout.svelte`

- [ ] **Step 1: Create `SiteHeader.svelte` (Nav markup verbatim)**

```svelte
<!-- site/src/lib/components/SiteHeader.svelte -->
<script lang="ts">
	import { Button } from '@rokkit/ui';
	import { ThemeSwitcherToggle } from '@rokkit/app';
	import BrandMark from './BrandMark.svelte';
	import ArrowIcon from './ArrowIcon.svelte';
	import { brand, nav } from '$lib/data';
</script>

<header
	class="sticky top-0 z-40 border-b border-paper-edge bg-paper/80 backdrop-blur-md backdrop-saturate-150"
>
	<div class="mx-auto flex max-w-content items-center justify-between gap-6 px-6 py-3.5">
		<a href="/#top" class="inline-flex items-center gap-2.5">
			<BrandMark class="h-7 w-7" />
			<span class="font-display font-semibold text-lg tracking-tight text-ink">{brand.name}</span>
		</a>
		<nav class="hidden items-center gap-7 md:flex">
			{#each nav.links as l (l.href)}
				<a
					href={l.href}
					class="whitespace-nowrap text-sm font-medium text-ink-mute transition-colors hover:text-ink"
					>{l.label}</a
				>
			{/each}
		</nav>
		<div class="flex items-center gap-3">
			<ThemeSwitcherToggle variant="single" size="md" />
			<div class="hidden sm:block">
				<Button href={nav.cta.href} variant="primary" size="sm">
					{nav.cta.label}
					<ArrowIcon />
				</Button>
			</div>
		</div>
	</div>
</header>
```

- [ ] **Step 2: Create `SiteFooter.svelte` (Footer markup verbatim)**

```svelte
<!-- site/src/lib/components/SiteFooter.svelte -->
<script lang="ts">
	import BrandMark from './BrandMark.svelte';
	import { brand, footer } from '$lib/data';
</script>

<footer class="border-t border-paper-edge bg-paper">
	<div class="mx-auto grid max-w-content gap-10 px-6 py-14 md:grid-cols-[1.5fr_1fr_1fr]">
		<div class="flex flex-col gap-3">
			<a href="/#top" class="inline-flex items-center gap-2.5">
				<BrandMark class="h-6 w-6" />
				<span class="font-display font-semibold text-lg tracking-tight text-ink">{brand.name}</span>
			</a>
			<p class="max-w-xs text-sm text-ink-mute text-pretty">{footer.tagline}</p>
		</div>
		{#each footer.columns as col (col.title)}
			<div class="flex flex-col gap-3">
				<span class="font-mono text-label uppercase text-ink-soft">{col.title}</span>
				<ul class="flex flex-col gap-2.5">
					{#each col.links as l (l.label)}
						<li>
							<a href={l.href} class="text-sm text-ink-mute transition-colors hover:text-ink"
								>{l.label}</a
							>
						</li>
					{/each}
				</ul>
			</div>
		{/each}
	</div>
	<div class="border-t border-paper-edge">
		<div
			class="mx-auto flex max-w-content flex-col gap-2 px-6 py-5 font-mono text-xs text-ink-soft sm:flex-row sm:items-center sm:justify-between"
		>
			<span>{footer.legal}</span>
			<span>v{__APP_VERSION__}</span>
		</div>
	</div>
</footer>
```

- [ ] **Step 3: Delete the old files and update the layout**

```bash
git rm site/src/lib/components/Nav.svelte site/src/lib/components/Footer.svelte
```

In `site/src/routes/+layout.svelte`, change the two imports:
`import Nav from '$lib/components/Nav.svelte';` → `import SiteHeader from '$lib/components/SiteHeader.svelte';`
`import Footer from '$lib/components/Footer.svelte';` → `import SiteFooter from '$lib/components/SiteFooter.svelte';`
and the markup `<Nav />` → `<SiteHeader />`, `<Footer />` → `<SiteFooter />`.

- [ ] **Step 4: Write the tests**

```ts
// site/src/lib/components/SiteHeader.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import SiteHeader from './SiteHeader.svelte';
import { brand, nav } from '$lib/data';

test('renders the brand, nav links and CTA', async () => {
	const screen = render(SiteHeader);
	await expect.element(screen.getByRole('banner')).toBeInTheDocument();
	await expect.element(screen.getByText(brand.name).first()).toBeInTheDocument();
	for (const l of nav.links) {
		await expect.element(screen.getByRole('link', { name: l.label })).toBeInTheDocument();
	}
	await expect.element(screen.getByText(nav.cta.label)).toBeInTheDocument();
});
```

```ts
// site/src/lib/components/SiteFooter.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import SiteFooter from './SiteFooter.svelte';
import { footer } from '$lib/data';

test('renders tagline, all column links, legal and the app version', async () => {
	const screen = render(SiteFooter);
	await expect.element(screen.getByRole('contentinfo')).toBeInTheDocument();
	await expect.element(screen.getByText(footer.tagline)).toBeInTheDocument();
	await expect.element(screen.getByText(footer.legal)).toBeInTheDocument();
	for (const col of footer.columns) {
		await expect.element(screen.getByText(col.title)).toBeInTheDocument();
	}
	// __APP_VERSION__ is injected by vite define; the footer prints "v<version>".
	expect(screen.container.textContent).toMatch(/v\d+\.\d+\.\d+/);
});
```

> If `ThemeSwitcherToggle` from `@rokkit/app` throws during render because it needs app setup, wrap the assertions in the header test to still verify the `banner` landmark, brand, and links (which render before the toggle), and open a follow-up note — do not weaken the other tests.

- [ ] **Step 5: Run + check**

```bash
cd site && bun run test -- src/lib/components/SiteHeader.svelte.test.ts src/lib/components/SiteFooter.svelte.test.ts && bun run check
```

Expected: PASS, 0 type errors (no dangling `Nav`/`Footer` refs).

- [ ] **Step 6: Commit**

```bash
git add site/src/lib/components/SiteHeader.svelte site/src/lib/components/SiteFooter.svelte site/src/lib/components/SiteHeader.svelte.test.ts site/src/lib/components/SiteFooter.svelte.test.ts site/src/routes/+layout.svelte
git commit -m "refactor(site): rename Nav/Footer to SiteHeader/SiteFooter + tests"
```

---

## Phase 4 — Architecture diagram (d3 port)

### Task 4.1: Add the `architecture` diagram data slice

**Files:**
- Modify: `site/src/lib/data.ts`

- [ ] **Step 1: Extend the existing `architecture` export with a typed diagram slice**

The current `architecture` export has only `eyebrow`/`title`/`caption`. Add the diagram data (code-derived: three backends, ~16 providers, capability traits). Replace the current `architecture` block in `data.ts` with:

```ts
export type ArchNode = { title: string; sub: string };
export type ArchDiagramData = {
	app: ArchNode;
	engine: { title: string; sub: string; caps: string[]; notes: string[] };
	adapter: { line1: string; line2: string; sub: string };
	cloud: { title: string; sub: string; pills: string[] };
	local: { title: string; sub: string; pills: string[] };
};

export const architecture = {
	eyebrow: 'Architecture',
	title: 'One adapter surface, many backends.',
	caption:
		'a small set of capability traits — cloud and local backends compose in a single routing config',
	diagram: {
		app: { title: 'Your app', sub: 'route(model)' },
		engine: {
			title: 'gateway',
			sub: 'routing engine',
			caps: ['fallback', 'circuit breaker', 'budget', 'tracing'],
			notes: ['no DB · GatewayStore trait', 'reqwest · rustls · tokio']
		},
		adapter: { line1: 'Inference', line2: 'Adapter', sub: 'capability traits' },
		cloud: {
			title: 'Cloud providers',
			sub: '~16 adapters',
			pills: ['openai', 'anthropic', 'gemini', 'bedrock', 'grok']
		},
		local: { title: 'Local engines', sub: 'in-process', pills: ['llama.cpp', 'onnx', 'fastembed'] }
	} as ArchDiagramData
};
```

- [ ] **Step 2: Type check**

```bash
cd site && bun run check
```

Expected: 0 errors.

- [ ] **Step 3: Commit**

```bash
git add site/src/lib/data.ts
git commit -m "feat(site): add architecture diagram data slice (code-derived)"
```

### Task 4.2: Build `ArchDiagram.svelte` (d3-selection, token colors, reduced-motion)

**Files:**
- Create: `site/src/lib/components/ArchDiagram.svelte`
- Create: `site/src/lib/components/ArchDiagram.svelte.test.ts`

Colors are read from probe elements carrying the site's token utility classes, so the diagram uses the exact resolved token colors in both themes without hardcoding CSS-var names. Redraw is keyed off `vibe.mode`.

- [ ] **Step 1: Create `ArchDiagram.svelte`**

```svelte
<!-- site/src/lib/components/ArchDiagram.svelte -->
<script lang="ts">
	import { select } from 'd3-selection';
	import { vibe } from '@rokkit/states';
	import { architecture, type ArchDiagramData } from '$lib/data';

	// `data` defaults to the diagram slice so the retained inline `<ArchDiagram />`
	// in +page.svelte keeps compiling until Task 4.3 swaps in ArchitectureSection.
	let { data = architecture.diagram }: { data?: ArchDiagramData } = $props();

	let host: HTMLDivElement;
	let probes: HTMLDivElement;

	const SG = "'Space Grotesk', sans-serif";
	const MONO = "'IBM Plex Mono', monospace";

	function readPalette() {
		// Each probe span carries a site token utility; read its resolved color.
		const q = (sel: string, prop: 'color' | 'backgroundColor' | 'borderTopColor') =>
			getComputedStyle(probes.querySelector(sel) as Element)[prop];
		return {
			fg: q('.text-ink', 'color'),
			muted: q('.text-ink-mute', 'color'),
			faint: q('.text-ink-soft', 'color'),
			accent: q('.text-primary', 'color'),
			accentWash: q('.bg-accent-soft', 'backgroundColor'),
			accentLine: q('.border-accent-line', 'borderTopColor'),
			surface: q('.bg-paper', 'backgroundColor'),
			line: q('.border-paper-edge', 'borderTopColor'),
			chip: q('.bg-paper-soft', 'backgroundColor')
		};
	}

	function draw() {
		if (!host || !probes) return;
		const reduce =
			typeof window !== 'undefined' &&
			window.matchMedia('(prefers-reduced-motion: reduce)').matches;
		const T = readPalette();
		const a = data;

		select(host).selectAll('*').remove();
		const svg = select(host)
			.append('svg')
			.attr('class', 'w-full h-auto overflow-visible')
			.attr('viewBox', '0 0 1080 440')
			.attr('preserveAspectRatio', 'xMidYMid meet')
			.attr('role', 'img')
			.attr('aria-label', 'gateway routes a model request through capability-trait adapters to cloud and local backends');

		const mk = svg
			.append('defs')
			.append('marker')
			.attr('id', 'gw-arw')
			.attr('viewBox', '0 0 8 8')
			.attr('refX', 6.4)
			.attr('refY', 4)
			.attr('markerWidth', 6)
			.attr('markerHeight', 6)
			.attr('orient', 'auto-start-reverse');
		mk.append('path').attr('d', 'M0.5,0.5 L7.5,4 L0.5,7.5 Z').style('fill', T.faint);

		const linkG = svg.append('g');
		const curve = (x1: number, y1: number, x2: number, y2: number) => {
			const mx = (x1 + x2) / 2;
			return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
		};
		([[172, 220, 246, 220], [582, 220, 636, 220], [772, 212, 826, 146], [772, 228, 826, 309]] as const).forEach(
			(l) => {
				const d = curve(l[0], l[1], l[2], l[3]);
				linkG
					.append('path')
					.attr('d', d)
					.style('fill', 'none')
					.style('stroke', T.line)
					.style('stroke-width', 1.75)
					.style('stroke-linecap', 'round')
					.attr('marker-end', 'url(#gw-arw)');
				const flow = linkG
					.append('path')
					.attr('d', d)
					.style('fill', 'none')
					.style('stroke', T.accent)
					.style('stroke-width', 2)
					.style('stroke-linecap', 'round')
					.style('stroke-dasharray', '0.5 13')
					.style('opacity', 0.5);
				if (!reduce) flow.style('animation', 'gw-flow 1.5s linear infinite');
			}
		);

		const cardRect = (g: any, x: number, y: number, w: number, h: number, accent: boolean) =>
			g
				.append('rect')
				.attr('x', x)
				.attr('y', y)
				.attr('width', w)
				.attr('height', h)
				.attr('rx', 16)
				.style('fill', accent ? T.accentWash : T.surface)
				.style('stroke', accent ? T.accentLine : T.line)
				.style('stroke-width', 1);
		const txt = (g: any, x: number, y: number, str: string, o: any = {}) =>
			g
				.append('text')
				.attr('x', x)
				.attr('y', y)
				.text(str)
				.style('font-family', o.mono ? MONO : SG)
				.style('font-size', `${o.size || 14}px`)
				.style('font-weight', o.weight || 600)
				.style('fill', o.fill || T.fg)
				.style('text-anchor', o.anchor || 'start')
				.style('letter-spacing', o.mono ? '0' : '-.01em');
		const pills = (g: any, labels: string[], x0: number, y0: number, maxX: number) => {
			let x = x0,
				y = y0;
			const h = 24,
				pad = 9,
				gap = 7;
			labels.forEach((l) => {
				const w = Math.round(l.length * 6.7) + pad * 2;
				if (x + w > maxX) {
					x = x0;
					y += h + 7;
				}
				g.append('rect').attr('x', x).attr('y', y).attr('width', w).attr('height', h).attr('rx', 7).style('fill', T.chip);
				g.append('text').attr('x', x + pad).attr('y', y + 16).text(l).style('font-family', MONO).style('font-size', '11.5px').style('fill', T.muted);
				x += w + gap;
			});
		};

		const app = svg.append('g');
		cardRect(app, 20, 170, 150, 100, false);
		txt(app, 95, 212, a.app.title, { size: 17, anchor: 'middle' });
		txt(app, 95, 236, a.app.sub, { mono: true, size: 12, weight: 500, fill: T.faint, anchor: 'middle' });

		const gw = svg.append('g');
		cardRect(gw, 250, 66, 330, 308, true);
		gw.append('circle').attr('cx', 300).attr('cy', 103).attr('r', 5).style('fill', T.accent);
		txt(gw, 315, 110, a.engine.title, { size: 23 });
		txt(gw, 315, 132, a.engine.sub, { mono: true, size: 12, weight: 500, fill: T.accent });
		gw.append('line').attr('x1', 274).attr('y1', 152).attr('x2', 556).attr('y2', 152).style('stroke', T.accentLine).style('stroke-width', 1);
		const capW = 262,
			capX = 284,
			capH = 27,
			capGap = 8,
			capY0 = 166;
		a.engine.caps.forEach((c, i) => {
			const y = capY0 + i * (capH + capGap);
			gw.append('rect').attr('x', capX).attr('y', y).attr('width', capW).attr('height', capH).attr('rx', 9).style('fill', T.surface).style('stroke', T.line).style('stroke-width', 1);
			gw.append('circle').attr('cx', capX + 16).attr('cy', y + capH / 2).attr('r', 3.5).style('fill', T.accent);
			txt(gw, capX + 30, y + capH / 2 + 4, c, { mono: true, size: 12, weight: 500, fill: T.muted });
		});
		gw.append('line').attr('x1', 274).attr('y1', 313).attr('x2', 556).attr('y2', 313).style('stroke', T.accentLine).style('stroke-width', 1);
		a.engine.notes.forEach((n, i) => txt(gw, 415, 334 + i * 19, n, { mono: true, size: 11, weight: 400, fill: T.faint, anchor: 'middle' }));

		const ad = svg.append('g');
		cardRect(ad, 640, 160, 130, 120, false);
		txt(ad, 705, 205, a.adapter.line1, { size: 15, anchor: 'middle' });
		txt(ad, 705, 225, a.adapter.line2, { size: 15, anchor: 'middle' });
		txt(ad, 705, 248, a.adapter.sub, { mono: true, size: 11.5, weight: 500, fill: T.accent, anchor: 'middle' });

		const cl = svg.append('g');
		cardRect(cl, 830, 70, 230, 150, false);
		txt(cl, 852, 104, a.cloud.title, { size: 15 });
		txt(cl, 852, 124, a.cloud.sub, { mono: true, size: 11.5, weight: 500, fill: T.accent });
		pills(cl, a.cloud.pills, 852, 140, 1044);

		const lo = svg.append('g');
		cardRect(lo, 830, 250, 230, 120, false);
		txt(lo, 852, 284, a.local.title, { size: 15 });
		txt(lo, 852, 304, a.local.sub, { mono: true, size: 11.5, weight: 500, fill: T.accent });
		pills(lo, a.local.pills, 852, 320, 1044);
	}

	// Redraw on mount and whenever the theme mode flips (re-reads token colors).
	$effect(() => {
		void vibe.mode;
		draw();
	});
</script>

<!-- Hidden probes: carry the site token utilities so d3 can read resolved colors. -->
<div bind:this={probes} aria-hidden="true" style="position:absolute; width:0; height:0; overflow:hidden; visibility:hidden">
	<span class="text-ink"></span>
	<span class="text-ink-mute"></span>
	<span class="text-ink-soft"></span>
	<span class="text-primary"></span>
	<span class="bg-accent-soft"></span>
	<span class="border-accent-line"></span>
	<span class="bg-paper"></span>
	<span class="border-paper-edge"></span>
	<span class="bg-paper-soft"></span>
</div>

<div bind:this={host} class="w-full min-h-75"></div>

<style>
	@keyframes gw-flow {
		to {
			stroke-dashoffset: -27;
		}
	}
</style>
```

- [ ] **Step 2: Write `ArchDiagram.svelte.test.ts`**

```ts
// site/src/lib/components/ArchDiagram.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';
import ArchDiagram from './ArchDiagram.svelte';
import { architecture } from '$lib/data';

test('renders an svg with the expected cards, capability rows and pills', async () => {
	const screen = render(ArchDiagram, { props: { data: architecture.diagram } });
	const svg = await vi.waitFor(() => {
		const el = screen.container.querySelector('svg');
		if (!el) throw new Error('svg not drawn yet');
		return el;
	});
	// 5 cards + 4 capability rects + provider/local pills.
	expect(svg.querySelectorAll('rect').length).toBeGreaterThanOrEqual(5 + 4);
	// Flow links: 4 base + 4 animated overlays = 8 paths (plus the marker path).
	expect(svg.querySelectorAll('path').length).toBeGreaterThanOrEqual(8);
	expect(svg.getAttribute('role')).toBe('img');
	expect(svg.textContent).toContain('gateway');
	expect(svg.textContent).toContain('~16 adapters');
});

test('renders each cloud provider pill label', async () => {
	const screen = render(ArchDiagram, { props: { data: architecture.diagram } });
	const svg = await vi.waitFor(() => {
		const el = screen.container.querySelector('svg');
		if (!el) throw new Error('not yet');
		return el;
	});
	for (const p of architecture.diagram.cloud.pills) {
		expect(svg.textContent).toContain(p);
	}
});
```

- [ ] **Step 3: Run tests + type check**

```bash
cd site && bun run test -- src/lib/components/ArchDiagram.svelte.test.ts && bun run check
```

Expected: PASS (2), 0 type errors.

- [ ] **Step 4: Commit**

```bash
git add site/src/lib/components/ArchDiagram.svelte site/src/lib/components/ArchDiagram.svelte.test.ts
git commit -m "feat(site): port d3 architecture diagram (token colors, reduced-motion) + tests"
```

### Task 4.3: `ArchitectureSection` block + wire into `+page.svelte`

**Files:**
- Create: `site/src/lib/components/ArchitectureSection.svelte`
- Create: `site/src/lib/components/ArchitectureSection.svelte.test.ts`
- Modify: `site/src/routes/+page.svelte`

- [ ] **Step 1: Create `ArchitectureSection.svelte`**

```svelte
<!-- site/src/lib/components/ArchitectureSection.svelte -->
<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import ArchDiagram from './ArchDiagram.svelte';
	import { architecture } from '$lib/data';
</script>

<section id="architecture" class="grid-section border-y border-paper-edge bg-paper-soft">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={architecture.eyebrow} title={architecture.title} />
		<div class="mt-12">
			<ArchDiagram data={architecture.diagram} />
			<p class="mt-6 text-center font-mono text-xs text-ink-soft">{architecture.caption}</p>
		</div>
	</div>
</section>
```

- [ ] **Step 2: Write `ArchitectureSection.svelte.test.ts`**

```ts
// site/src/lib/components/ArchitectureSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';
import ArchitectureSection from './ArchitectureSection.svelte';
import { architecture } from '$lib/data';

test('renders the heading, caption and the diagram svg', async () => {
	const screen = render(ArchitectureSection);
	await expect.element(screen.getByText(architecture.title)).toBeInTheDocument();
	await expect.element(screen.getByText(architecture.caption)).toBeInTheDocument();
	await vi.waitFor(() => {
		if (!screen.container.querySelector('svg')) throw new Error('diagram not drawn');
	});
});
```

- [ ] **Step 3: Replace the inline ARCHITECTURE block in `+page.svelte`**

Remove the current inline `<!-- ARCHITECTURE -->` `<section id="architecture" …>…</section>` (and its now-unused `SectionHead`/`ArchDiagram`/`architecture` references from the old inline version), and ensure the composition list from Task 3.8 includes `import ArchitectureSection from '$lib/components/ArchitectureSection.svelte';` and `<ArchitectureSection />` in position (between `UsageSection` and `ConsumersSection`).

- [ ] **Step 4: Confirm no stale references**

`ArchDiagram.svelte` was rewritten in Task 4.2 (same path, d3 content, `data` optional). After removing the inline architecture block, `+page.svelte` no longer imports `SectionHead`, `ArchDiagram`, or `architecture` — verify with a type check:

```bash
cd site && bun run check
```

Expected: 0 errors.

- [ ] **Step 5: Run the section test + full suite**

```bash
cd site && bun run test -- src/lib/components/ArchitectureSection.svelte.test.ts && bun run test
```

Expected: section PASS (1), full suite green.

- [ ] **Step 6: Commit**

```bash
git add site/src/lib/components/ArchitectureSection.svelte site/src/lib/components/ArchitectureSection.svelte.test.ts site/src/routes/+page.svelte
git commit -m "feat(site): ArchitectureSection block wired into composition + test"
```

---

## Phase 5 — Final verification

### Task 5.1: Full-suite + build + composition-overflow guard

**Files:**
- Create: `site/src/routes/page.svelte.test.ts` (a page-level composition smoke + overflow check)

- [ ] **Step 1: Write a page composition test (renders all blocks, asserts no horizontal overflow)**

```ts
// site/src/routes/page.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';

vi.mock('$app/state', () => ({
	page: { url: new URL('https://gateway.sensei-hq.com/') }
}));

import Page from './+page.svelte';

test('the page renders all sections without horizontal overflow', async () => {
	render(Page);
	// Every section id from the composition should be present.
	for (const id of ['features', 'crates', 'usage', 'architecture', 'consumers', 'versioning', 'start']) {
		expect(document.querySelector(`#${id}`)).not.toBeNull();
	}
	// No horizontal overflow: the document isn't wider than the viewport.
	await vi.waitFor(() => {
		const el = document.documentElement;
		expect(el.scrollWidth).toBeLessThanOrEqual(el.clientWidth + 1);
	});
});
```

- [ ] **Step 2: Run the whole suite**

```bash
cd site && bun run test
```

Expected: all client + server tests green.

- [ ] **Step 3: Type check + production build**

```bash
cd site && bun run check && bun run build
```

Expected: `svelte-check` 0 errors, `vite build` completes (prerender succeeds).

- [ ] **Step 4: Manually confirm the running site (evidence before done)**

```bash
cd site && bun run preview
```

Load the preview URL, toggle light/dark, and confirm: hero + all sections render, the architecture diagram draws and re-themes, code tabs switch, copy works. Stop the preview when done.

- [ ] **Step 5: Commit**

```bash
git add site/src/routes/page.svelte.test.ts
git commit -m "test(site): page composition + no-overflow guard"
```

### Task 5.2: Update the component inventory doc (optional but recommended)

**Files:**
- Modify: `docs/mockups/COMPONENTS.md` (or add a short `site/src/lib/components/README.md`)

- [ ] **Step 1: Note the 1:1 name map is now realized in `site/`**

Add a short note that the app now mirrors the mockup inventory (`SiteHeader`, `SiteFooter`, `InfoCard`, `CodeFrame`, `HeroSection … CtaSection`, `ArchitectureSection` + internal `ArchDiagram`), with data code-derived from `data.ts`.

- [ ] **Step 2: Commit**

```bash
git add docs/mockups/COMPONENTS.md
git commit -m "docs(site): note app now mirrors the mockup component inventory"
```

---

## Self-review notes (traceability to spec)

- Spec §4 (target tree), §4.1 rename map → Tasks 2.1, 2.2, 3.9.
- Spec §4.2 section blocks + §4.3 composition split → Tasks 3.1–3.8.
- Spec §5 CodeFrame consolidation → Task 2.2.
- Spec §6 d3 ArchitectureSection + §7 data slice → Tasks 4.1–4.3.
- Spec §8 testing (harness + per-component coverage + computed-style) → Task 0.x (harness), 1.x/2.x/3.x/4.x (per-component), computed-style in 2.1 (border), 5.1 (overflow); theme-color check exercised via ArchDiagram redraw (Task 4.2) and manual verification (5.1 Step 4).
- Spec §9 sequencing (green at each step) → phase ordering; every task ends on green + commit.
- Spec §10 non-goals respected: no content rewrite (data additive only), no token migration, no route changes, no Rust changes, no e2e suite.
- Spec §11 DoD → Task 5.1 (full suite + build + manual confirm).

**Known follow-ups (surface, don't silently absorb):** if `@rokkit/app` `ThemeSwitcherToggle` needs runtime setup in browser tests, Task 3.9 Step 4 keeps the header test meaningful (landmark/brand/links) and flags it; the exact Rokkit token→CSS var names are avoided entirely by the probe-element approach in Task 4.2.
