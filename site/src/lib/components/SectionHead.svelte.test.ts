import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import SectionHead from './SectionHead.svelte';

test('renders eyebrow and title, omits lede when empty', async () => {
	const screen = await render(SectionHead, {
		eyebrow: 'The routing engine',
		title: 'Everything a request needs.'
	});
	await expect.element(screen.getByText('The routing engine')).toBeInTheDocument();
	await expect.element(screen.getByRole('heading', { level: 2 })).toBeInTheDocument();
	expect(screen.container.querySelector('p')).toBeNull();
});

test('renders the lede paragraph when provided', async () => {
	const screen = await render(SectionHead, { eyebrow: 'E', title: 'T', lede: 'No database of its own.' });
	await expect.element(screen.getByText('No database of its own.')).toBeInTheDocument();
});

test('center align adds the centering classes', async () => {
	const screen = await render(SectionHead, { eyebrow: 'E', title: 'T', align: 'center' });
	expect(screen.container.querySelector('.text-center')).not.toBeNull();
});
