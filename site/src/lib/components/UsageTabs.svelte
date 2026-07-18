<script lang="ts">
	import type { UsageTab } from '$lib/data';

	let { tabs }: { tabs: UsageTab[] } = $props();
	let activeId = $state<string | null>(null);
	const active = $derived(activeId ?? tabs[0]?.id);
	const current = $derived(tabs.find((t) => t.id === active) ?? tabs[0]);
</script>

<div
	class="code-surface overflow-hidden"
	style="border-radius:14px; border:1px solid var(--code-border); box-shadow: var(--code-shadow)"
>
	<div
		class="flex items-center"
		style="background: var(--code-bar); gap:2px; padding:8px 10px 0; border-bottom:1px solid var(--code-border)"
		role="tablist"
	>
		{#each tabs as t (t.id)}
			<button
				role="tab"
				aria-selected={t.id === active}
				onclick={() => (activeId = t.id)}
				class="font-mono"
				style="appearance:none; border:none; cursor:pointer; font-size:13px; font-weight:500; padding:9px 14px; border-radius:8px 8px 0 0; {t.id ===
				active
					? 'color: var(--code-text); background: var(--code-bg);'
					: 'color: var(--code-idle); background: transparent;'}"
			>
				{t.label}
			</button>
		{/each}
	</div>
	<pre
		class="font-mono"
		style="margin:0; background: var(--code-bg); color: var(--code-text); padding:22px 20px; font-size:13px; line-height:1.85; overflow-x:auto; min-height:150px">{current.code}</pre>
</div>
