// site/src/lib/components/ArchDiagram.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';
import ArchDiagram from './ArchDiagram.svelte';
import { architecture } from '$lib/data';

test('renders an svg with the expected cards, capability rows and pills', async () => {
	const screen = await render(ArchDiagram, { data: architecture.diagram });
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
	const screen = await render(ArchDiagram, { data: architecture.diagram });
	const svg = await vi.waitFor(() => {
		const el = screen.container.querySelector('svg');
		if (!el) throw new Error('not yet');
		return el;
	});
	for (const p of architecture.diagram.cloud.pills) {
		expect(svg.textContent).toContain(p);
	}
});

test('defines a global "gw-flow" keyframes rule so the d3 flow animation runs', async () => {
	// The d3 code applies `animation: gw-flow …` by literal name; if the component
	// <style> scoped the keyframes (svelte-<hash>-gw-flow) the animation would be
	// dead. Assert an un-hashed `gw-flow` keyframes rule reaches the document.
	await render(ArchDiagram, { data: architecture.diagram });
	const hasGlobalKeyframes = Array.from(document.styleSheets).some((sheet) => {
		let rules: CSSRuleList;
		try {
			rules = sheet.cssRules;
		} catch {
			return false; // cross-origin sheet — skip
		}
		return Array.from(rules).some(
			(r) => r instanceof CSSKeyframesRule && r.name === 'gw-flow'
		);
	});
	expect(hasGlobalKeyframes).toBe(true);
});
