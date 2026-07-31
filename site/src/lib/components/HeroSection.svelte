<script lang="ts">
	import { Button } from '@rokkit/ui';
	import CodeFrame from './CodeFrame.svelte';
	import ArrowIcon from './ArrowIcon.svelte';
	import { hero } from '$lib/data';
</script>

<section class="relative overflow-hidden">
	<div class="pointer-events-none absolute inset-0 bg-grid mask-fade-b opacity-60"></div>
	<div
		class="anim-rise relative mx-auto flex max-w-content flex-col items-center gap-6 px-6 pb-16 pt-20 text-center md:pt-28"
	>
		<div
			class="inline-flex items-center gap-2 rounded-full border border-accent-line bg-accent-soft px-3 py-1 text-sm font-semibold text-primary"
		>
			<span class="font-mono">{hero.badge[0]}</span>
			<span class="text-ink-soft">·</span>
			{hero.badge[1]}
		</div>
		<h1 class="max-w-3xl font-display font-semibold text-display text-ink text-balance">
			{hero.title}
		</h1>
		<p class="max-w-2xl text-lg text-ink-mute text-pretty">{hero.lede}</p>
		<div class="flex flex-wrap items-center justify-center gap-3 pt-1">
			<Button href={hero.primaryCta.href} variant="primary" size="lg">
				{hero.primaryCta.label}
				<ArrowIcon />
			</Button>
			<Button href={hero.secondaryCta.href} variant="default" style="outline" size="lg">
				{hero.secondaryCta.label}
			</Button>
		</div>
		<div class="mt-6 w-full max-w-2xl text-left">
			<CodeFrame filename={hero.code.filename} code={hero.code.source} />
		</div>
	</div>
</section>
