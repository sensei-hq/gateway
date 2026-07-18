<script lang="ts">
	import Eyebrow from '$lib/components/Eyebrow.svelte';
	import Seo from '$lib/components/Seo.svelte';
	import { pages } from '$lib/docs';
</script>

<Seo
	title="Docs — gateway"
	description="Usage guides for the gateway crate: quickstart, configuration, routing recipes, embedded + Hugging Face download, custom adapters, and the 0.2 → 0.3 upgrade."
/>

<section class="mx-auto max-w-content px-6 py-section">
	<div class="flex flex-col gap-4">
		<Eyebrow>Documentation</Eyebrow>
		<h1 class="font-display font-semibold text-h2 text-ink text-balance">Docs</h1>
		<p class="max-w-2xl text-lg text-ink-mute text-pretty">
			Task-oriented guides for using the <span class="font-mono text-primary">gateway</span> crate —
			written to be easy for people and coding agents to follow. Also available as
			<a href="/llms.txt" class="text-primary hover:underline">/llms.txt</a> and
			<a href="/llms-full.txt" class="text-primary hover:underline">/llms-full.txt</a>.
		</p>
	</div>

	<div class="mt-12 grid gap-4 sm:grid-cols-2">
		{#each pages as p (p.slug)}
			<a
				href="/docs/{p.slug}"
				class="group flex items-center justify-between gap-4 rounded-xl border border-paper-edge bg-paper-mute p-6 transition-colors hover:border-accent"
			>
				<div class="flex flex-col gap-1">
					<span class="font-mono text-xs text-ink-soft">{String(p.order + 1).padStart(2, '0')}</span>
					<span class="font-display font-semibold text-xl text-ink">{p.title}</span>
					<span class="text-sm text-ink-mute text-pretty">{p.description}</span>
				</div>
				<span class="text-primary transition-transform group-hover:translate-x-1">→</span>
			</a>
		{/each}
	</div>
</section>
