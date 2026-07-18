<script lang="ts">
	import type { UsageTab } from '$lib/data';

	let { tabs }: { tabs: UsageTab[] } = $props();
	let activeId = $state<string | null>(null);
	const active = $derived(activeId ?? tabs[0]?.id);
	const current = $derived(tabs.find((t) => t.id === active) ?? tabs[0]);
</script>

<div class="overflow-hidden rounded-xl border border-paper-edge bg-paper-soft">
	<div class="flex items-center gap-1 border-b border-paper-edge bg-paper-mute px-2 pt-2" role="tablist">
		{#each tabs as t (t.id)}
			<button
				role="tab"
				aria-selected={t.id === active}
				onclick={() => (activeId = t.id)}
				class="rounded-t-md px-3.5 py-2 font-mono text-sm transition-colors {t.id === active
					? 'bg-paper-soft text-ink'
					: 'text-ink-soft hover:text-ink-mute'}"
			>
				{t.label}
			</button>
		{/each}
	</div>
	<pre
		class="overflow-x-auto px-5 py-5 font-mono text-[0.82rem] leading-loose text-ink">{current.code}</pre>
</div>
