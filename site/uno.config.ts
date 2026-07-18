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
	}
});
