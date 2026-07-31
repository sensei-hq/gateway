<script lang="ts">
	import { Button } from '@rokkit/ui';
	import Seo from '$lib/components/Seo.svelte';
	import SectionHead from '$lib/components/SectionHead.svelte';
	import ArchDiagram from '$lib/components/ArchDiagram.svelte';
	import ArrowIcon from '$lib/components/ArrowIcon.svelte';
	import HeroSection from '$lib/components/HeroSection.svelte';
	import ProofStrip from '$lib/components/ProofStrip.svelte';
	import FeaturesSection from '$lib/components/FeaturesSection.svelte';
	import CratesSection from '$lib/components/CratesSection.svelte';
	import UsageSection from '$lib/components/UsageSection.svelte';
	import ConsumersSection from '$lib/components/ConsumersSection.svelte';
	import VersioningSection from '$lib/components/VersioningSection.svelte';
	import { architecture, start } from '$lib/data';

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

<!-- ARCHITECTURE -->
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

<!-- CTA -->
<section id="start" class="grid-section">
	<div
		class="mx-auto flex max-w-content flex-col items-center gap-5 px-6 py-section text-center"
	>
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
