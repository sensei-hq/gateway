<script lang="ts">
	import { proof } from '$lib/data';
</script>

<div class="border-y border-paper-edge bg-paper-soft">
	<div
		class="mx-auto flex max-w-content flex-wrap items-center gap-x-6 gap-y-2 px-6 py-4 font-mono text-sm text-ink-soft"
	>
		<span class="font-medium text-ink-mute">{proof.label}</span>
		{#each proof.providers as p (p)}
			<span>{p}</span>
		{/each}
	</div>
</div>
