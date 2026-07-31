// site/src/lib/components/UsageSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import UsageSection from './UsageSection.svelte';
import { usage } from '$lib/data';

test('renders the heading, a tablist with every usage tab, and the note', async () => {
	const screen = await render(UsageSection);
	await expect.element(screen.getByText(usage.title)).toBeInTheDocument();
	await expect.element(screen.getByRole('tablist')).toBeInTheDocument();
	for (const t of usage.tabs) {
		await expect.element(screen.getByRole('tab', { name: t.label })).toBeInTheDocument();
	}
	await expect.element(screen.getByText(usage.note)).toBeInTheDocument();
});
