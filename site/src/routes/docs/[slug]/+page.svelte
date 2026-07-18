<script lang="ts">
	import { pages, findPage } from '$lib/docs';
	import Seo from '$lib/components/Seo.svelte';

	let { data }: { data: { slug: string } } = $props();

	const current = $derived(findPage(data.slug));
	const idx = $derived(pages.findIndex((p) => p.slug === data.slug));
	const prev = $derived(idx > 0 ? pages[idx - 1] : undefined);
	const next = $derived(idx >= 0 && idx < pages.length - 1 ? pages[idx + 1] : undefined);
</script>

<Seo
	title="{current?.title ?? 'Docs'} — gateway"
	description={current?.description ?? 'gateway documentation.'}
	type="article"
/>

<div class="mx-auto grid max-w-content gap-12 px-6 py-section md:grid-cols-[16rem_1fr]">
	<!-- Sidebar -->
	<aside class="md:sticky md:top-24 md:self-start">
		<a href="/docs" class="font-mono text-label uppercase text-ink-soft hover:text-ink">Docs</a>
		<nav class="mt-4 flex flex-col gap-1">
			{#each pages as p (p.slug)}
				<a
					href="/docs/{p.slug}"
					aria-current={p.slug === data.slug ? 'page' : undefined}
					class="rounded-md px-3 py-2 text-sm transition-colors {p.slug === data.slug
						? 'bg-accent-soft text-primary font-medium'
						: 'text-ink-mute hover:bg-paper-soft hover:text-ink'}"
				>
					{p.title}
				</a>
			{/each}
		</nav>
	</aside>

	<!-- Content -->
	<article class="min-w-0">
		{#if current}
			<!-- eslint-disable-next-line svelte/no-at-html-tags — trusted build-time docs from docs/llms -->
			<div class="doc-prose text-ink-mute">{@html current.html}</div>
		{/if}

		<div class="mt-16 flex items-center justify-between gap-4 border-t border-paper-edge pt-6">
			{#if prev}
				<a href="/docs/{prev.slug}" class="text-sm text-ink-mute hover:text-primary"
					>← {prev.title}</a
				>
			{:else}
				<span></span>
			{/if}
			{#if next}
				<a href="/docs/{next.slug}" class="text-sm text-ink-mute hover:text-primary"
					>{next.title} →</a
				>
			{/if}
		</div>
	</article>
</div>

<style>
	.doc-prose {
		line-height: 1.7;
		font-size: 0.95rem;
	}
	.doc-prose :global(h1) {
		font-family: '"Space Grotesk"', 'Space Grotesk', system-ui, sans-serif;
		font-size: 2.1rem;
		font-weight: 700;
		letter-spacing: -0.02em;
		line-height: 1.1;
		color: var(--k-ink, inherit);
		margin: 0 0 1.25rem;
	}
	.doc-prose :global(h2) {
		font-family: '"Space Grotesk"', 'Space Grotesk', system-ui, sans-serif;
		font-size: 1.45rem;
		font-weight: 600;
		letter-spacing: -0.015em;
		color: var(--k-ink, inherit);
		margin: 2.75rem 0 1rem;
	}
	.doc-prose :global(h3) {
		font-family: '"Space Grotesk"', 'Space Grotesk', system-ui, sans-serif;
		font-size: 1.15rem;
		font-weight: 600;
		color: var(--k-ink, inherit);
		margin: 2rem 0 0.75rem;
	}
	.doc-prose :global(p),
	.doc-prose :global(ul),
	.doc-prose :global(ol),
	.doc-prose :global(table) {
		margin: 0 0 1.1rem;
	}
	.doc-prose :global(ul),
	.doc-prose :global(ol) {
		padding-left: 1.4rem;
	}
	.doc-prose :global(li) {
		margin: 0.35rem 0;
	}
	.doc-prose :global(a) {
		color: oklch(0.55 0.13 245);
		text-decoration: none;
	}
	.doc-prose :global(a:hover) {
		text-decoration: underline;
	}
	:global([data-mode='dark']) .doc-prose :global(a) {
		color: oklch(0.78 0.12 245);
	}
	.doc-prose :global(strong) {
		font-weight: 600;
		color: var(--k-ink, inherit);
	}
	.doc-prose :global(code) {
		font-family: '"IBM Plex Mono"', 'IBM Plex Mono', ui-monospace, monospace;
		font-size: 0.85em;
		padding: 0.12rem 0.35rem;
		border-radius: 0.35rem;
		background: rgba(127, 127, 127, 0.12);
	}
	.doc-prose :global(pre) {
		font-family: '"IBM Plex Mono"', 'IBM Plex Mono', ui-monospace, monospace;
		font-size: 0.8rem;
		line-height: 1.65;
		padding: 1.1rem 1.25rem;
		border-radius: 0.85rem;
		overflow-x: auto;
		background: rgba(127, 127, 127, 0.1);
		border: 1px solid rgba(127, 127, 127, 0.2);
		margin: 0 0 1.4rem;
	}
	.doc-prose :global(pre code) {
		padding: 0;
		background: none;
		font-size: inherit;
	}
	.doc-prose :global(table) {
		width: 100%;
		border-collapse: collapse;
		font-size: 0.88rem;
		display: block;
		overflow-x: auto;
	}
	.doc-prose :global(th),
	.doc-prose :global(td) {
		text-align: left;
		padding: 0.5rem 0.75rem;
		border: 1px solid rgba(127, 127, 127, 0.2);
	}
	.doc-prose :global(blockquote) {
		border-left: 3px solid oklch(0.55 0.13 245 / 0.5);
		padding-left: 1rem;
		margin: 0 0 1.1rem;
		opacity: 0.85;
	}
</style>
