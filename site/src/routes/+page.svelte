<script lang="ts">
	import Seo from '$lib/components/Seo.svelte';
	import HeroSection from '$lib/components/HeroSection.svelte';
	import ProofStrip from '$lib/components/ProofStrip.svelte';
	import FeaturesSection from '$lib/components/FeaturesSection.svelte';
	import CratesSection from '$lib/components/CratesSection.svelte';
	import UsageSection from '$lib/components/UsageSection.svelte';
	import ArchitectureSection from '$lib/components/ArchitectureSection.svelte';
	import ConsumersSection from '$lib/components/ConsumersSection.svelte';
	import VersioningSection from '$lib/components/VersioningSection.svelte';
	import CtaSection from '$lib/components/CtaSection.svelte';

	const description =
		'gateway is a provider-agnostic multimodal inference routing engine for Rust — routing chat, embeddings, image, video and speech across ~16 cloud providers plus in-process local models, with fallback chains, per-endpoint circuit breaker, budget management, multi-model consensus and request tracing behind one trait-based config.';
</script>

<Seo title="gateway — multimodal inference routing engine for Rust" {description} />

<span id="top"></span>

<HeroSection />
<ProofStrip />
<FeaturesSection />
<CratesSection />
<UsageSection />
<ArchitectureSection />
<ConsumersSection />
<VersioningSection />
<CtaSection />
