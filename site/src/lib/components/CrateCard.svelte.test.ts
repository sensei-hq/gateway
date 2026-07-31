import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import CrateCard from './CrateCard.svelte';

const crate = {
	name: 'local-providers',
	version: 'v0.4.8',
	body: 'In-process inference adapters.',
	chips: ['llama-cpp', 'fastembed', 'ort'],
	note: 'feature-gated'
};

test('renders name, version chip, body, dep chips and note', async () => {
	const screen = await render(CrateCard, { crate });
	await expect.element(screen.getByText('local-providers')).toBeInTheDocument();
	await expect.element(screen.getByText('v0.4.8')).toBeInTheDocument();
	await expect.element(screen.getByText('In-process inference adapters.')).toBeInTheDocument();
	await expect.element(screen.getByText('llama-cpp')).toBeInTheDocument();
	await expect.element(screen.getByText('feature-gated')).toBeInTheDocument();
});

test('omits the note span when the crate has no note', async () => {
	const screen = await render(CrateCard, { crate: { ...crate, note: undefined } });
	expect(screen.container.textContent).not.toContain('feature-gated');
});
