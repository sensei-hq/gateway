// site/src/lib/components/ArchDiagram.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';
import ArchDiagram from './ArchDiagram.svelte';
import { architecture } from '$lib/data';

test('renders an svg with the expected cards, capability rows and pills', async () => {
	const screen = await render(ArchDiagram, { props: { data: architecture.diagram } });
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
	const screen = await render(ArchDiagram, { props: { data: architecture.diagram } });
	const svg = await vi.waitFor(() => {
		const el = screen.container.querySelector('svg');
		if (!el) throw new Error('not yet');
		return el;
	});
	for (const p of architecture.diagram.cloud.pills) {
		expect(svg.textContent).toContain(p);
	}
});
