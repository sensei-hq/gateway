import { defineConfig } from 'unocss';
import { presetRokkit } from '@rokkit/unocss';
import rokkitConfig from './rokkit.config.js';

// presetRokkit bundles presetWind3 + presetIcons + presetTypography + the
// Svelte extractor, generates the z-scale semantic utilities from
// rokkit.config.js, and wires dark mode to [data-mode="dark"].
export default defineConfig({
	presets: [presetRokkit(rokkitConfig)],
	theme: {
		fontFamily: {
			display: ['"Space Grotesk"', 'system-ui', 'sans-serif'],
			sans: ['"IBM Plex Sans"', 'system-ui', 'sans-serif'],
			mono: ['"IBM Plex Mono"', 'ui-monospace', 'monospace']
		},
		maxWidth: { content: '76rem' }
	},
	content: {
		// The Vite plugin normally learns which utilities exist by watching files
		// as Vite transforms them, so `virtual:uno.css` is only as complete as
		// whatever has been transformed *so far*. Component browser tests render a
		// single component in isolation (no +layout.svelte), and the test's
		// setup file imports `virtual:uno.css` before the component under test
		// has been transformed — so transform-order scanning alone leaves it
		// incomplete (e.g. missing `border-paper-edge`) on that first request.
		// An explicit filesystem scan makes the generated CSS complete
		// regardless of import order.
		filesystem: ['src/**/*.svelte']
	}
});
