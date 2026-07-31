import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import Chip from './Chip.svelte';

test('renders the label', async () => {
	const screen = await render(Chip, { label: 'tokio' });
	await expect.element(screen.getByText('tokio')).toBeInTheDocument();
});

test('accent tone uses the accent-soft surface class', async () => {
	const screen = await render(Chip, { label: 'v0.4.8', tone: 'accent' });
	const chip = screen.getByText('v0.4.8').element();
	expect(chip.className).toContain('bg-accent-soft');
});

test('default tone uses the paper-soft surface class', async () => {
	const screen = await render(Chip, { label: 'reqwest' });
	const chip = screen.getByText('reqwest').element();
	expect(chip.className).toContain('bg-paper-soft');
});
