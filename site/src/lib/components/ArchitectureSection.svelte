<script lang="ts">
	import SectionHead from './SectionHead.svelte';
	import ArchDiagram from './ArchDiagram.svelte';
	import { architecture } from '$lib/data';
</script>

<section id="architecture" class="grid-section border-y border-line bg-paper-soft">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={architecture.eyebrow} title={architecture.title} />
		<div class="mt-12">
			<ArchDiagram data={architecture.diagram} />
			<p class="mt-6 text-center font-mono text-xs text-ink-soft">{architecture.caption}</p>
		</div>
	</div>
</section>
