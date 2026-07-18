<script lang="ts">
	let { label, tone = 'default' }: { label: string; tone?: 'default' | 'accent' } = $props();
</script>

<span
	class="inline-flex items-center rounded-md px-2 py-1 font-mono text-xs {tone === 'accent'
		? 'bg-accent-soft text-primary'
		: 'bg-paper-soft text-ink-mute'}"
>
	{label}
</span>
