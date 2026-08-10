import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import BrandMark from './BrandMark.svelte';

test('on-blue variant renders the rounded accent square', async () => {
	const screen = await render(BrandMark, { variant: 'on-blue' });
	const svg = screen.container.querySelector('svg');
	expect(svg?.querySelector('rect')).not.toBeNull();
	expect(svg?.getAttribute('aria-hidden')).toBe('true');
});

test('white variant renders the transparent glyph (no square)', async () => {
	const screen = await render(BrandMark, { variant: 'white' });
	const svg = screen.container.querySelector('svg');
	expect(svg?.querySelector('rect')).toBeNull();
	expect(svg?.querySelectorAll('path').length).toBeGreaterThan(0);
});

test('accepts a custom class', async () => {
	const screen = await render(BrandMark, { class: 'h-7 w-7' });
	expect(screen.container.querySelector('svg')?.getAttribute('class')).toContain('h-7');
});
