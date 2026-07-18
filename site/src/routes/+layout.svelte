<script lang="ts">
	import 'virtual:uno.css';
	import '@fontsource/space-grotesk/400.css';
	import '@fontsource/space-grotesk/500.css';
	import '@fontsource/space-grotesk/600.css';
	import '@fontsource/space-grotesk/700.css';
	import '@fontsource/ibm-plex-sans/400.css';
	import '@fontsource/ibm-plex-sans/500.css';
	import '@fontsource/ibm-plex-sans/600.css';
	import '@fontsource/ibm-plex-mono/400.css';
	import '@fontsource/ibm-plex-mono/500.css';
	import '../app.css';
	import { vibe } from '@rokkit/states';
	import { themable } from '@rokkit/actions';
	import { browser } from '$app/environment';
	import Nav from '$lib/components/Nav.svelte';
	import Footer from '$lib/components/Footer.svelte';

	let { children } = $props();

	// Seed vibe from the mode the pre-paint themeHook resolved, then let
	// `use:themable` own the bridge (loads storage, writes data-mode/style to
	// <body>+<html>, persists, syncs cross-tab). The nav's ThemeSwitcherToggle
	// drives vibe. This app ships only the zen-sumi style, so lock vibe to it.
	if (browser) {
		vibe.allowedStyles = ['zen-sumi'];
		vibe.style = 'zen-sumi';
		const dm = document.documentElement.dataset.mode;
		if (dm === 'light' || dm === 'dark') vibe.mode = dm;
	}
</script>

<svelte:body use:themable={{ theme: vibe, storageKey: 'gateway-theme' }} />

<div class="flex min-h-screen flex-col">
	<Nav />
	<main class="flex-1">
		{@render children()}
	</main>
	<Footer />
</div>
