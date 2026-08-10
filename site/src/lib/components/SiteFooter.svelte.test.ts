// site/src/lib/components/SiteFooter.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import SiteFooter from './SiteFooter.svelte';
import { footer } from '$lib/data';

test('renders tagline, all column links, legal and the app version', async () => {
	const screen = await render(SiteFooter);
	await expect.element(screen.getByRole('contentinfo')).toBeInTheDocument();
	await expect.element(screen.getByText(footer.tagline)).toBeInTheDocument();
	await expect.element(screen.getByText(footer.legal)).toBeInTheDocument();
	for (const col of footer.columns) {
		await expect.element(screen.getByText(col.title)).toBeInTheDocument();
	}
	// __APP_VERSION__ is injected by vite define; the footer prints "v<version>".
	expect(screen.container.textContent).toMatch(/v\d+\.\d+\.\d+/);
});
