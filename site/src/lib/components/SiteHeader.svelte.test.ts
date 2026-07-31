// site/src/lib/components/SiteHeader.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { page } from 'vitest/browser';
import { expect, test } from 'vitest';
import SiteHeader from './SiteHeader.svelte';
import { brand, nav } from '$lib/data';

test('renders the brand, nav links and CTA', async () => {
	// The nav links use `hidden md:flex`; the default test viewport (414x896)
	// is below the `md` breakpoint, so widen it to desktop to make them visible.
	await page.viewport(1280, 800);
	const screen = await render(SiteHeader);
	await expect.element(screen.getByRole('banner')).toBeInTheDocument();
	await expect.element(screen.getByText(brand.name).first()).toBeInTheDocument();
	for (const l of nav.links) {
		await expect.element(screen.getByRole('link', { name: l.label })).toBeInTheDocument();
	}
	await expect.element(screen.getByText(nav.cta.label)).toBeInTheDocument();
});
