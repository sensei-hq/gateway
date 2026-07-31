import { render } from 'vitest-browser-svelte';
import { expect, test, vi, beforeEach } from 'vitest';
import CodeFrame from './CodeFrame.svelte';

beforeEach(() => {
	vi.restoreAllMocks();
});

test('chrome mode: shows filename, copy button and code', async () => {
	const screen = await render(CodeFrame, { filename: 'Cargo.toml', code: '[dependencies]' });
	await expect.element(screen.getByText('Cargo.toml')).toBeInTheDocument();
	await expect.element(screen.getByText('[dependencies]')).toBeInTheDocument();
	await expect.element(screen.getByRole('button', { name: /copy/i })).toBeInTheDocument();
	expect(screen.container.querySelector('[role="tablist"]')).toBeNull();
});

test('chrome mode: copy writes the code to the clipboard and flips the label', async () => {
	const writeText = vi.fn().mockResolvedValue(undefined);
	// `navigator.clipboard` is a getter-only accessor on the real (Playwright)
	// Navigator, so a plain Object.assign throws; redefine the property instead.
	Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true });
	const screen = await render(CodeFrame, { filename: 'f', code: 'cargo build' });
	await screen.getByRole('button', { name: /copy/i }).click();
	expect(writeText).toHaveBeenCalledWith('cargo build');
	await expect.element(screen.getByText('Copied')).toBeInTheDocument();
});

test('tabs mode: renders a tablist and switches active code on click', async () => {
	const tabs = [
		{ id: 'add', label: 'Cargo.toml', code: 'ADD_CODE' },
		{ id: 'patch', label: 'Local dev', code: 'PATCH_CODE' }
	];
	const screen = await render(CodeFrame, { tabs });
	await expect.element(screen.getByRole('tablist')).toBeInTheDocument();
	await expect.element(screen.getByText('ADD_CODE')).toBeInTheDocument();
	await screen.getByRole('tab', { name: 'Local dev' }).click();
	await expect.element(screen.getByText('PATCH_CODE')).toBeInTheDocument();
});

test('tabs mode: aria-selected tracks the active tab', async () => {
	const tabs = [
		{ id: 'a', label: 'A', code: 'x' },
		{ id: 'b', label: 'B', code: 'y' }
	];
	const screen = await render(CodeFrame, { tabs, activeTab: 'b' });
	const tabB = screen.getByRole('tab', { name: 'B' }).element();
	expect(tabB.getAttribute('aria-selected')).toBe('true');
});
