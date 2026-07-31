<script lang="ts">
	import Seo from '$lib/components/Seo.svelte';
	import SectionHead from '$lib/components/SectionHead.svelte';
	import ArchDiagram from '$lib/components/ArchDiagram.svelte';
	import HeroSection from '$lib/components/HeroSection.svelte';
	import ProofStrip from '$lib/components/ProofStrip.svelte';
	import FeaturesSection from '$lib/components/FeaturesSection.svelte';
	import CratesSection from '$lib/components/CratesSection.svelte';
	import UsageSection from '$lib/components/UsageSection.svelte';
	import ConsumersSection from '$lib/components/ConsumersSection.svelte';
	import VersioningSection from '$lib/components/VersioningSection.svelte';
	import CtaSection from '$lib/components/CtaSection.svelte';
	import { architecture } from '$lib/data';

	const description =
		'gateway is a provider-agnostic LLM inference routing engine for Rust — fallback chains, per-endpoint circuit breaker, budget management and request tracing across ~16 cloud providers plus in-process local models, behind one trait-based config.';
</script>

<Seo title="gateway — LLM inference routing engine for Rust" {description} />

<span id="top"></span>

<HeroSection />
<ProofStrip />
<FeaturesSection />
<CratesSection />
<UsageSection />

<!-- ARCHITECTURE — extracted into ArchitectureSection in Task 4.3 -->
<section id="architecture" class="grid-section border-y border-paper-edge bg-paper-soft">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={architecture.eyebrow} title={architecture.title} />
		<div class="mt-12">
			<ArchDiagram />
			<p class="mt-6 text-center font-mono text-xs text-ink-soft">{architecture.caption}</p>
		</div>
	</div>
</section>

<ConsumersSection />
<VersioningSection />
<CtaSection />
