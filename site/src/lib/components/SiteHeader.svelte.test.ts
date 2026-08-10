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

test('sticky header has a near-opaque fill so it stays readable over dark content', async () => {
	const screen = await render(SiteHeader);
	const header = screen.getByRole('banner').element() as HTMLElement;
	const bg = getComputedStyle(header).backgroundColor;
	// Must resolve to an actual color with high alpha — NOT transparent. (The old
	// `bg-paper/NN` opacity modifier produced an invalid rule → transparent header,
	// which showed the dark Versioning panel through it.)
	expect(bg).not.toBe('rgba(0, 0, 0, 0)');
	expect(bg).not.toBe('transparent');
	const alphaMatch = bg.match(/[\d.]+\s*\)$/);
	if (alphaMatch) {
		const alpha = parseFloat(alphaMatch[0]);
		expect(alpha).toBeGreaterThan(0.85);
	}
});
