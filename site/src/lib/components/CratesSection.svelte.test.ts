// site/src/lib/components/CratesSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import CratesSection from './CratesSection.svelte';
import { crates } from '$lib/data';

test('renders the heading and one card per crate', async () => {
	const screen = await render(CratesSection);
	await expect.element(screen.getByText(crates.title)).toBeInTheDocument();
	for (const c of crates.items) {
		// exact match: the crate name span (a name like "vault" also appears as a
		// substring in its own description body, so a loose match is ambiguous).
		await expect.element(screen.getByText(c.name, { exact: true })).toBeInTheDocument();
	}
});
