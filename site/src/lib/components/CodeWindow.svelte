<script lang="ts">
	let { filename, code }: { filename: string; code: string } = $props();
	let copied = $state(false);

	async function copy() {
		try {
			await navigator.clipboard.writeText(code);
			copied = true;
			setTimeout(() => (copied = false), 1400);
		} catch {
			/* clipboard unavailable — no-op */
		}
	}
</script>

<div class="overflow-hidden rounded-xl border border-paper-edge bg-paper-soft shadow-2xl">
	<div class="flex items-center justify-between gap-3 border-b border-paper-edge px-4 py-2.5">
		<div class="flex items-center gap-1.5">
			<span class="h-2.5 w-2.5 rounded-full bg-paper-mute"></span>
			<span class="h-2.5 w-2.5 rounded-full bg-paper-mute"></span>
			<span class="h-2.5 w-2.5 rounded-full bg-paper-mute"></span>
		</div>
		<span class="font-mono text-xs text-ink-soft">{filename}</span>
		<button
			onclick={copy}
			class="font-mono text-xs text-ink-soft transition-colors hover:text-primary"
			aria-label="Copy code to clipboard"
		>
			{copied ? 'copied ✓' : 'copy'}
		</button>
	</div>
	<pre
		class="overflow-x-auto px-5 py-4 font-mono text-[0.82rem] leading-relaxed text-ink">{code}</pre>
</div>
