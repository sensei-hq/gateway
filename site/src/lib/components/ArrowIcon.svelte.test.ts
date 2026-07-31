import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import ArrowIcon from './ArrowIcon.svelte';

test('renders an inline svg arrow', async () => {
	// vitest-browser-svelte's `render` is async (it returns a Promise<RenderResult>),
	// so it must be awaited before the container/locators are usable.
	const screen = await render(ArrowIcon);
	const svg = screen.container.querySelector('svg');
	expect(svg).not.toBeNull();
	expect(svg?.querySelector('path')).not.toBeNull();
});
