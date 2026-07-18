<script lang="ts">
	import Eyebrow from './Eyebrow.svelte';

	let {
		eyebrow,
		title,
		lede = '',
		align = 'left'
	}: { eyebrow: string; title: string; lede?: string; align?: 'left' | 'center' } = $props();

	const alignCls = $derived(
		align === 'center' ? 'items-center text-center mx-auto' : 'items-start'
	);
</script>

<div class="flex flex-col gap-4 {alignCls}">
	<Eyebrow>{eyebrow}</Eyebrow>
	<h2 class="font-display font-semibold text-h2 text-ink max-w-2xl text-balance">{title}</h2>
	{#if lede}
		<p class="text-lg text-ink-mute max-w-2xl text-pretty">{lede}</p>
	{/if}
</div>
