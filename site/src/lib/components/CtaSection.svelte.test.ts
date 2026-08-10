// site/src/lib/components/CtaSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import CtaSection from './CtaSection.svelte';
import { start } from '$lib/data';

test('renders the CTA title, lede and both action buttons', async () => {
	const screen = await render(CtaSection);
	await expect.element(screen.getByText(start.title)).toBeInTheDocument();
	await expect.element(screen.getByText(start.lede)).toBeInTheDocument();
	await expect.element(screen.getByText(start.primaryCta.label)).toBeInTheDocument();
	await expect.element(screen.getByText(start.secondaryCta.label)).toBeInTheDocument();
});
