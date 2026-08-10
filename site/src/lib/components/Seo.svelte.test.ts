import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';

vi.mock('$app/state', () => ({ page: { url: new URL('https://gateway.sensei-hq.com/') } }));

import Seo from './Seo.svelte';

test('sets document title, description and canonical meta', async () => {
	await render(Seo, { title: 'gateway — routing', description: 'Provider-agnostic.' });
	expect(document.title).toBe('gateway — routing');
	const desc = document.head.querySelector('meta[name="description"]');
	expect(desc?.getAttribute('content')).toBe('Provider-agnostic.');
	const canonical = document.head.querySelector('link[rel="canonical"]');
	expect(canonical?.getAttribute('href')).toContain('gateway.sensei-hq.com');
});

test('adds a noindex robots meta when noindex is set', async () => {
	await render(Seo, { title: 'T', description: 'D', noindex: true });
	const robots = document.head.querySelector('meta[name="robots"]');
	expect(robots?.getAttribute('content')).toBe('noindex');
});
