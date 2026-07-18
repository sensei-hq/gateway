import { sveltekit } from '@sveltejs/kit/vite';
import UnoCSS from 'unocss/vite';
import { defineConfig } from 'vite';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// Single source of truth for the displayed version — `make bump` keeps
// package.json in sync with Cargo.toml, so the footer always matches the release.
const { version } = JSON.parse(
	readFileSync(fileURLToPath(new URL('./package.json', import.meta.url)), 'utf-8')
);

export default defineConfig({
	define: {
		__APP_VERSION__: JSON.stringify(version)
	},
	plugins: [UnoCSS(), sveltekit()]
});
