// site/src/routes/page.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { page } from 'vitest/browser';
import { expect, test, vi } from 'vitest';

vi.mock('$app/state', () => ({
	page: { url: new URL('https://gateway.sensei-hq.com/') }
}));

import Page from './+page.svelte';

test('the page renders all sections without horizontal overflow', async () => {
	// The default test viewport (414x896) is mobile; widen to a common desktop
	// width so the no-overflow assertion is meaningful for the primary layout.
	await page.viewport(1280, 800);
	await render(Page);
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

test('does not overflow horizontally at a mobile viewport either', async () => {
	await page.viewport(414, 896);
	await render(Page);
	await vi.waitFor(() => {
		const el = document.documentElement;
		expect(el.scrollWidth).toBeLessThanOrEqual(el.clientWidth + 1);
	});
});
