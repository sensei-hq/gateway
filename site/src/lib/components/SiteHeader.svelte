<script lang="ts">
	import { Button } from '@rokkit/ui';
	import { ThemeSwitcherToggle } from '@rokkit/app';
	import BrandMark from './BrandMark.svelte';
	import ArrowIcon from './ArrowIcon.svelte';
	import { brand, nav } from '$lib/data';
</script>

<header
	class="sticky top-0 z-40 border-b border-paper-edge bg-paper/80 backdrop-blur-md backdrop-saturate-150"
>
	<div class="mx-auto flex max-w-content items-center justify-between gap-6 px-6 py-3.5">
		<a href="/#top" class="inline-flex items-center gap-2.5">
			<BrandMark class="h-7 w-7" />
			<span class="font-display font-semibold text-lg tracking-tight text-ink">{brand.name}</span>
		</a>
		<nav class="hidden items-center gap-7 md:flex">
			{#each nav.links as l (l.href)}
				<a
					href={l.href}
					class="whitespace-nowrap text-sm font-medium text-ink-mute transition-colors hover:text-ink"
					>{l.label}</a
				>
			{/each}
		</nav>
		<div class="flex items-center gap-3">
			<ThemeSwitcherToggle variant="single" size="md" />
			<div class="hidden sm:block">
				<Button href={nav.cta.href} variant="primary" size="sm">
					{nav.cta.label}
					<ArrowIcon />
				</Button>
			</div>
		</div>
	</div>
</header>
