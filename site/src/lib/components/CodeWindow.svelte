<script lang="ts">
	let { filename, code }: { filename: string; code: string } = $props();
	let copied = $state(false);

	async function copy() {
		try {
			await navigator.clipboard.writeText(code);
			copied = true;
			setTimeout(() => (copied = false), 1500);
		} catch {
			/* clipboard unavailable — no-op */
		}
	}
</script>

<div
	class="code-surface overflow-hidden"
	style="border-radius:14px; border:1px solid var(--code-border); box-shadow: var(--code-shadow)"
>
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
		<button
			onclick={copy}
			class="font-mono"
			style="margin-left:auto; appearance:none; border:none; cursor:pointer; font-size:11.5px; color: var(--code-copy-text); background: var(--code-copy-bg); padding:4px 10px; border-radius:6px"
			aria-label="Copy code to clipboard"
		>
			{copied ? 'Copied' : 'Copy'}
		</button>
	</div>
	<pre
		class="font-mono"
		style="margin:0; background: var(--code-bg); color: var(--code-text); padding:20px; font-size:13.5px; line-height:1.8; overflow-x:auto">{code}</pre>
</div>
