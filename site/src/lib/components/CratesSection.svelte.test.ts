// site/src/lib/components/CratesSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import CratesSection from './CratesSection.svelte';
import { crates } from '$lib/data';

test('renders the heading and one card per crate', async () => {
	const screen = await render(CratesSection);
	await expect.element(screen.getByText(crates.title)).toBeInTheDocument();
	for (const c of crates.items) {
		await expect.element(screen.getByText(c.name)).toBeInTheDocument();
	}
});
