// site/src/lib/components/ArchitectureSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test, vi } from 'vitest';
import ArchitectureSection from './ArchitectureSection.svelte';
import { architecture } from '$lib/data';

test('renders the heading, caption and the diagram svg', async () => {
	const screen = await render(ArchitectureSection);
	await expect.element(screen.getByText(architecture.title)).toBeInTheDocument();
	await expect.element(screen.getByText(architecture.caption)).toBeInTheDocument();
	await vi.waitFor(() => {
		if (!screen.container.querySelector('svg')) throw new Error('diagram not drawn');
	});
});
