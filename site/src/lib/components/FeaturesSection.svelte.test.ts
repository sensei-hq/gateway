// site/src/lib/components/FeaturesSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import FeaturesSection from './FeaturesSection.svelte';
import { features } from '$lib/data';

test('renders the section heading and one InfoCard per feature', async () => {
	const screen = await render(FeaturesSection);
	await expect.element(screen.getByText(features.title)).toBeInTheDocument();
	for (const f of features.items) {
		await expect.element(screen.getByText(f.title)).toBeInTheDocument();
	}
	// One h3 heading per feature card.
	expect(screen.container.querySelectorAll('h3').length).toBe(features.items.length);
});
