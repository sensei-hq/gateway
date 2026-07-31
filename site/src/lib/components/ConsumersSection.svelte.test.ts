// site/src/lib/components/ConsumersSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import ConsumersSection from './ConsumersSection.svelte';
import { consumers } from '$lib/data';

test('renders a linked card per consumer with repo href', async () => {
	const screen = await render(ConsumersSection);
	await expect.element(screen.getByText(consumers.title)).toBeInTheDocument();
	// Both consumer repos live under the same `sensei-hq` org, so a name-based
	// role locator (e.g. /sensei/) matches more than one card (Playwright
	// strict-mode violation). Look the link up by its exact href instead.
	for (const c of consumers.items) {
		const link = screen.container.querySelector(`a[href="${c.repo}"]`);
		expect(link).not.toBeNull();
		expect(link?.textContent).toContain(c.name);
	}
});
