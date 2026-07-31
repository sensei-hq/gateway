// site/src/lib/components/ProofStrip.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import ProofStrip from './ProofStrip.svelte';
import { proof } from '$lib/data';

test('renders the label and every provider', async () => {
	const screen = await render(ProofStrip);
	await expect.element(screen.getByText(proof.label)).toBeInTheDocument();
	for (const p of proof.providers) {
		await expect.element(screen.getByText(p)).toBeInTheDocument();
	}
});
