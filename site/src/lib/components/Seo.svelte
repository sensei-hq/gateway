<script lang="ts">
	import { page } from '$app/state';
	import { canonicalFor, OG_IMAGE } from '$lib/seo';

	// Per-page SEO: title, description, canonical URL, and Open Graph / Twitter
	// metadata. Site-wide constants (og:site_name, default og:image, twitter:card,
	// keywords, theme-color) live in app.html; this component sets everything that
	// varies per page so social shares + canonical links are page-accurate.
	let {
		title,
		description,
		type = 'website',
		image = OG_IMAGE,
		noindex = false
	}: {
		title: string;
		description: string;
		type?: 'website' | 'article';
		image?: string;
		noindex?: boolean;
	} = $props();

	const canonical = $derived(canonicalFor(page.url.pathname));
</script>

<svelte:head>
	<title>{title}</title>
	<meta name="description" content={description} />
	<link rel="canonical" href={canonical} />
	{#if noindex}<meta name="robots" content="noindex" />{/if}

	<meta property="og:type" content={type} />
	<meta property="og:title" content={title} />
	<meta property="og:description" content={description} />
	<meta property="og:url" content={canonical} />
	<meta property="og:image" content={image} />

	<meta name="twitter:title" content={title} />
	<meta name="twitter:description" content={description} />
	<meta name="twitter:image" content={image} />
</svelte:head>
