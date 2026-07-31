<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import { consumers } from '$lib/data';
</script>

<section id="consumers" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={consumers.eyebrow} title={consumers.title} lede={consumers.lede} />
		<div class="mt-12 grid gap-6 sm:grid-cols-2">
			{#each consumers.items as c (c.name)}
				<a
					href={c.repo}
					class="flex items-center gap-4 rounded-xl border border-paper-edge bg-paper-mute p-7 transition-colors hover:border-accent"
				>
					<span
						class="grid h-11 w-11 shrink-0 place-items-center rounded-lg border border-accent-line bg-accent-soft font-mono text-xl font-semibold text-primary"
					>
						{c.glyph}
					</span>
					<div>
						<div class="font-display font-semibold text-lg text-ink">{c.name}</div>
						<div class="font-mono text-sm text-ink-soft">
							{c.repo.replace('https://', '')}
						</div>
					</div>
				</a>
			{/each}
		</div>
	</div>
</section>
