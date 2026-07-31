// site/src/lib/components/HeroSection.svelte.test.ts
import { render } from 'vitest-browser-svelte';
import { expect, test } from 'vitest';
import HeroSection from './HeroSection.svelte';
import { hero } from '$lib/data';

test('renders the hero title, lede and both CTAs from data', async () => {
	const screen = await render(HeroSection);
	await expect.element(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
	await expect.element(screen.getByText(hero.title)).toBeInTheDocument();
	await expect.element(screen.getByText(hero.lede)).toBeInTheDocument();
	await expect.element(screen.getByText(hero.primaryCta.label)).toBeInTheDocument();
	await expect.element(screen.getByText(hero.secondaryCta.label)).toBeInTheDocument();
});

test('embeds the install CodeFrame with the Cargo.toml filename', async () => {
	const screen = await render(HeroSection);
	await expect.element(screen.getByText(hero.code.filename)).toBeInTheDocument();
});
