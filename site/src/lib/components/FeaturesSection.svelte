<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import InfoCard from './InfoCard.svelte';
	import { features } from '$lib/data';
</script>

<section id="features" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={features.eyebrow} title={features.title} lede={features.lede} />
		<div class="mt-12 grid gap-4 sm:grid-cols-2 md:grid-cols-3">
			{#each features.items as f (f.tag)}
				<InfoCard tag={f.tag} title={f.title} body={f.body} />
			{/each}
		</div>
	</div>
</section>
