<script lang="ts">
	type Tab = { id: string; label: string; code: string };
	let {
		filename = 'code',
		code = '',
		copyable = true,
		tabs,
		activeTab,
		onSelectTab
	}: {
		filename?: string;
		code?: string;
		copyable?: boolean;
		tabs?: Tab[];
		activeTab?: string;
		onSelectTab?: (id: string) => void;
	} = $props();

	let internalTab = $state<string | null>(null);
	const hasTabs = $derived(!!tabs && tabs.length > 0);
	const activeId = $derived(activeTab ?? internalTab ?? tabs?.[0]?.id);
	const current = $derived(tabs?.find((t) => t.id === activeId) ?? tabs?.[0]);
	const shownCode = $derived(hasTabs ? (current?.code ?? '') : code);

	let copied = $state(false);
	async function copy() {
		try {
			await navigator.clipboard.writeText(shownCode);
			copied = true;
			setTimeout(() => (copied = false), 1500);
		} catch {
			/* clipboard unavailable — no-op */
		}
	}

	function selectTab(id: string) {
		internalTab = id;
		onSelectTab?.(id);
	}
</script>

<div
	class="code-surface overflow-hidden"
	style="border-radius:14px; border:1px solid var(--code-border); box-shadow: var(--code-shadow)"
>
	{#if hasTabs}
		<div
			class="flex items-center"
			style="background: var(--code-bar); gap:2px; padding:8px 10px 0; border-bottom:1px solid var(--code-border)"
			role="tablist"
		>
			{#each tabs! as t (t.id)}
				<button
					role="tab"
					aria-selected={t.id === activeId}
					onclick={() => selectTab(t.id)}
					class="font-mono"
					style="appearance:none; border:none; cursor:pointer; font-size:13px; font-weight:500; padding:9px 14px; border-radius:8px 8px 0 0; {t.id ===
					activeId
						? 'color: var(--code-text); background: var(--code-bg);'
						: 'color: var(--code-idle); background: transparent;'}"
				>
					{t.label}
				</button>
			{/each}
		</div>
	{:else}
		<div
			class="flex items-center gap-2"
			style="background: var(--code-bar); padding:11px 14px; border-bottom:1px solid var(--code-border)"
		>
			<span style="width:11px;height:11px;border-radius:50%;background:#ff5f57"></span>
			<span style="width:11px;height:11px;border-radius:50%;background:#febc2e"></span>
			<span style="width:11px;height:11px;border-radius:50%;background:#28c840"></span>
			<span class="font-mono" style="font-size:12.5px; color: var(--code-idle); margin-left:6px"
				>{filename}</span
			>
			{#if copyable}
				<button
					onclick={copy}
					class="font-mono"
					style="margin-left:auto; appearance:none; border:none; cursor:pointer; font-size:11.5px; color: var(--code-copy-text); background: var(--code-copy-bg); padding:4px 10px; border-radius:6px"
					aria-label="Copy code to clipboard"
				>
					{copied ? 'Copied' : 'Copy'}
				</button>
			{/if}
		</div>
	{/if}
	<pre
		class="font-mono"
		style="margin:0; background: var(--code-bg); color: var(--code-text); padding:20px; font-size:13.5px; line-height:1.8; overflow-x:auto; min-height:150px">{shownCode}</pre>
</div>
