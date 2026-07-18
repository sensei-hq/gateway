<script lang="ts">
	let { tag, title, body }: { tag: string; title: string; body: string } = $props();
</script>

<div
	class="group flex flex-col gap-3 rounded-xl border border-paper-edge bg-paper-mute p-6 transition-colors hover:border-accent"
>
	<span class="font-mono text-xs text-ink-soft transition-colors group-hover:text-primary">{tag}</span>
	<h3 class="font-display font-semibold text-xl text-ink">{title}</h3>
	<p class="text-sm text-ink-mute text-pretty">{body}</p>
</div>
