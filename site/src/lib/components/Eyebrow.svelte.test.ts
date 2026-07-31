import { render } from 'vitest-browser-svelte';
import { createRawSnippet } from 'svelte';
import { expect, test } from 'vitest';
import Eyebrow from './Eyebrow.svelte';

const label = createRawSnippet(() => ({ render: () => `<span>Routing engine</span>` }));

test('renders its child label and the dot marker', async () => {
	const screen = await render(Eyebrow, { children: label });
	await expect.element(screen.getByText('Routing engine')).toBeInTheDocument();
	expect(screen.container.querySelector('span.bg-primary')).not.toBeNull();
});
