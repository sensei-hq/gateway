<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import CrateCard from './CrateCard.svelte';
	import { crates } from '$lib/data';
</script>

<section id="crates" class="grid-section border-y border-line bg-paper-soft">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={crates.eyebrow} title={crates.title} />
		<div class="mt-12 grid gap-6 md:grid-cols-2">
			{#each crates.items as c (c.name)}
				<CrateCard crate={c} />
			{/each}
		</div>
	</div>
</section>
