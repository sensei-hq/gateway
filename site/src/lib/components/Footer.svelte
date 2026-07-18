<script lang="ts">
	import BrandMark from './BrandMark.svelte';
	import { brand, footer } from '$lib/data';
</script>

<footer class="border-t border-paper-edge bg-paper">
	<div class="mx-auto grid max-w-content gap-10 px-6 py-14 md:grid-cols-[1.5fr_1fr_1fr]">
		<div class="flex flex-col gap-3">
			<a href="/#top" class="inline-flex items-center gap-2.5">
				<BrandMark class="h-6 w-6" />
				<span class="font-display font-semibold text-lg tracking-tight text-ink">{brand.name}</span>
			</a>
			<p class="max-w-xs text-sm text-ink-mute text-pretty">{footer.tagline}</p>
		</div>
		{#each footer.columns as col (col.title)}
			<div class="flex flex-col gap-3">
				<span class="font-mono text-label uppercase text-ink-soft">{col.title}</span>
				<ul class="flex flex-col gap-2.5">
					{#each col.links as l (l.label)}
						<li>
							<a href={l.href} class="text-sm text-ink-mute transition-colors hover:text-ink"
								>{l.label}</a
							>
						</li>
					{/each}
				</ul>
			</div>
		{/each}
	</div>
	<div class="border-t border-paper-edge">
		<div
			class="mx-auto flex max-w-content flex-col gap-2 px-6 py-5 font-mono text-xs text-ink-soft sm:flex-row sm:items-center sm:justify-between"
		>
			<span>{footer.legal}</span>
			<span>v{__APP_VERSION__}</span>
		</div>
	</div>
</footer>
