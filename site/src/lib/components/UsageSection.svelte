<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import CodeFrame from './CodeFrame.svelte';
	import { usage } from '$lib/data';
</script>

<section id="usage" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={usage.eyebrow} title={usage.title} lede={usage.lede} />
		<div class="mt-10">
			<CodeFrame tabs={usage.tabs} />
			<p class="mt-4 px-1 font-mono text-sm text-ink-soft">{usage.note}</p>
		</div>
	</div>
</section>
