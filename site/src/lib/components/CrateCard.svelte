<script lang="ts">
	import Chip from './Chip.svelte';
	import type { Crate } from '$lib/data';

	let { crate }: { crate: Crate } = $props();
</script>

<div
	class="flex flex-col gap-4 rounded-xl border border-paper-edge bg-paper-mute p-8 transition-colors hover:border-accent"
>
	<div class="flex items-center justify-between gap-3">
		<span class="font-mono font-semibold text-xl text-ink">{crate.name}</span>
		<Chip label={crate.version} tone="accent" />
	</div>
	<p class="text-sm text-ink-mute text-pretty leading-relaxed">{crate.body}</p>
	<div class="flex flex-wrap items-center gap-2">
		{#each crate.chips as c (c)}
			<Chip label={c} />
		{/each}
		{#if crate.note}
			<span class="px-1 font-mono text-xs text-ink-soft">{crate.note}</span>
		{/if}
	</div>
</div>
