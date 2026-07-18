<script lang="ts">
	const caps = ['fallback', 'circuit breaker', 'budget', 'tracing'];
	const cloud = ['openai', 'anthropic', 'gemini', 'bedrock', 'grok'];
	const local = ['llama.cpp', 'onnx', 'fastembed'];
</script>

{#snippet arrow()}
	<div class="flex shrink-0 items-center justify-center text-ink-soft">
		<svg
			class="h-4 w-4 rotate-90 lg:rotate-0"
			viewBox="0 0 16 16"
			fill="none"
			stroke="currentColor"
			stroke-width="1.6"
			aria-hidden="true"
		>
			<path d="M3 8h10M9 4l4 4-4 4" stroke-linecap="round" stroke-linejoin="round" />
		</svg>
	</div>
{/snippet}

<div class="flex flex-col items-stretch gap-3 lg:flex-row lg:items-center">
	<!-- Your app -->
	<div
		class="flex shrink-0 flex-col gap-1 rounded-xl border border-paper-edge bg-paper p-5 text-center lg:w-36"
	>
		<span class="font-display font-semibold text-ink">Your app</span>
		<span class="font-mono text-xs text-ink-soft">route(model)</span>
	</div>

	{@render arrow()}

	<!-- gateway -->
	<div class="flex-1 rounded-xl border border-accent-line bg-accent-soft p-6">
		<div class="flex items-center gap-2">
			<span class="h-2 w-2 rounded-full bg-primary"></span>
			<span class="font-display font-semibold text-lg text-ink">gateway</span>
			<span class="font-mono text-xs text-primary">routing engine</span>
		</div>
		<div class="mt-4 grid gap-2 sm:grid-cols-2">
			{#each caps as c (c)}
				<div class="flex items-center gap-2 rounded-md border border-paper-edge bg-paper px-3 py-2">
					<span class="h-1.5 w-1.5 rounded-full bg-primary"></span>
					<span class="font-mono text-xs text-ink-mute">{c}</span>
				</div>
			{/each}
		</div>
		<div
			class="mt-4 border-t border-accent-line pt-3 text-center font-mono text-xs text-ink-soft"
		>
			no DB · GatewayStore trait · reqwest · rustls · tokio
		</div>
	</div>

	{@render arrow()}

	<!-- Inference adapter -->
	<div
		class="flex shrink-0 flex-col gap-1 rounded-xl border border-paper-edge bg-paper p-5 text-center lg:w-36"
	>
		<span class="font-display font-semibold leading-tight text-ink">Inference adapter</span>
		<span class="font-mono text-xs text-primary">capability traits</span>
	</div>

	{@render arrow()}

	<!-- backends -->
	<div class="flex shrink-0 flex-col gap-3 lg:w-56">
		<div class="rounded-xl border border-paper-edge bg-paper p-4">
			<div class="font-display font-semibold text-sm text-ink">Cloud providers</div>
			<div class="font-mono text-xs text-primary">~16 adapters</div>
			<div class="mt-2 flex flex-wrap gap-1.5">
				{#each cloud as p (p)}
					<span class="rounded bg-paper-mute px-1.5 py-0.5 font-mono text-[0.7rem] text-ink-mute"
						>{p}</span
					>
				{/each}
			</div>
		</div>
		<div class="rounded-xl border border-paper-edge bg-paper p-4">
			<div class="font-display font-semibold text-sm text-ink">Local engines</div>
			<div class="font-mono text-xs text-primary">in-process</div>
			<div class="mt-2 flex flex-wrap gap-1.5">
				{#each local as p (p)}
					<span class="rounded bg-paper-mute px-1.5 py-0.5 font-mono text-[0.7rem] text-ink-mute"
						>{p}</span
					>
				{/each}
			</div>
		</div>
	</div>
</div>
