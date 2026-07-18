import { themeHook } from '@rokkit/unocss/hooks';

// Injects a pre-paint script that applies the persisted Rokkit theme
// (data-mode / data-style / data-density) before first paint — no flash.
// Shares the 'gateway-theme' storage key with the vibe store (see +layout.svelte).
export const handle = themeHook({
	storageKey: 'gateway-theme',
	defaultMode: 'system',
	defaultStyle: 'zen-sumi',
	defaultDensity: 'comfortable'
});
