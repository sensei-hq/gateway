<script lang="ts">
	import { vibe } from '@rokkit/states';
	import Eyebrow from './Eyebrow.svelte';
	import { versioning } from '$lib/data';

	// The versioning panel flips to the opposite mode of the page (mockup detail).
	const invMode = $derived(vibe.mode === 'dark' ? 'light' : 'dark');
</script>

<section
	id="versioning"
	data-mode={invMode}
	class="grid-section border-y border-line bg-paper"
>
	<div class="mx-auto grid max-w-content items-center gap-10 px-6 py-section md:grid-cols-2 md:gap-14">
		<div class="flex flex-col gap-4">
			<Eyebrow>{versioning.eyebrow}</Eyebrow>
			<h2 class="font-display font-semibold text-h2 text-ink text-balance">{versioning.title}</h2>
			<p class="max-w-xl text-lg text-ink-mute text-pretty">{versioning.lede}</p>
		</div>
		<div class="flex flex-col gap-3">
			{#each versioning.steps as s (s.n)}
				<div class="flex items-start gap-3 rounded-lg border border-paper-edge bg-paper-mute px-5 py-4">
					<span class="font-mono text-sm text-primary">{s.n}</span>
					<div>
						<div class="font-display font-semibold text-ink">{s.title}</div>
						<div class="mt-1 font-mono text-sm text-ink-soft">{s.note}</div>
					</div>
				</div>
			{/each}
		</div>
	</div>
</section>
