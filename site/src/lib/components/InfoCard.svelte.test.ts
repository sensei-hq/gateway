import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import InfoCard from './InfoCard.svelte';

test('renders tag, title and body', async () => {
	const screen = await render(InfoCard, {
		tag: 'fallback',
		title: 'Named fallback chains',
		body: 'Chain endpoints by name.'
	});
	await expect.element(screen.getByText('fallback', { exact: true })).toBeInTheDocument();
	await expect.element(screen.getByRole('heading', { level: 3 })).toBeInTheDocument();
	await expect.element(screen.getByText('Chain endpoints by name.')).toBeInTheDocument();
});

test('card border computes to a real 1px solid line (not collapsed)', async () => {
	const screen = await render(InfoCard, { tag: 't', title: 'T', body: 'B' });
	const card = screen.container.querySelector('div')!;
	const cs = getComputedStyle(card);
	expect(cs.borderTopStyle).toBe('solid');
	expect(parseFloat(cs.borderTopWidth)).toBeCloseTo(1, 1);
	expect(cs.boxSizing).toBe('border-box');
});
