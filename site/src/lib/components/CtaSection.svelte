<script lang="ts">
	import { Button } from '@rokkit/ui';
	import ArrowIcon from './ArrowIcon.svelte';
	import { start } from '$lib/data';
</script>

<section id="start" class="grid-section">
	<div class="mx-auto flex max-w-content flex-col items-center gap-5 px-6 py-section text-center">
		<h2 class="max-w-2xl font-display font-semibold text-h2 text-ink text-balance">{start.title}</h2>
		<p class="max-w-xl text-lg text-ink-mute text-pretty">{start.lede}</p>
		<div class="flex flex-wrap items-center justify-center gap-3 pt-2">
			<Button href={start.primaryCta.href} variant="primary" size="lg">
				{start.primaryCta.label}
				<ArrowIcon />
			</Button>
			<Button href={start.secondaryCta.href} variant="default" style="outline" size="lg">
				{start.secondaryCta.label}
			</Button>
		</div>
	</div>
</section>
