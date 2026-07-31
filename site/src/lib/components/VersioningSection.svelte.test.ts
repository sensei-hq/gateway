// site/src/lib/components/VersioningSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import VersioningSection from './VersioningSection.svelte';
import { versioning } from '$lib/data';

test('renders the title and every versioning step', async () => {
	const screen = await render(VersioningSection);
	await expect.element(screen.getByText(versioning.title)).toBeInTheDocument();
	for (const s of versioning.steps) {
		await expect.element(screen.getByText(s.title)).toBeInTheDocument();
		await expect.element(screen.getByText(s.n)).toBeInTheDocument();
	}
});

test('sets a data-mode attribute on the section (inverted panel)', async () => {
	const screen = await render(VersioningSection);
	const section = screen.container.querySelector('#versioning')!;
	expect(['light', 'dark']).toContain(section.getAttribute('data-mode'));
});
