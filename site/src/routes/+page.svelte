<script lang="ts">
	import { Button } from '@rokkit/ui';
	import { vibe } from '@rokkit/states';
	import Seo from '$lib/components/Seo.svelte';
	import Eyebrow from '$lib/components/Eyebrow.svelte';
	import SectionHead from '$lib/components/SectionHead.svelte';
	import FeatureCard from '$lib/components/FeatureCard.svelte';
	import CrateCard from '$lib/components/CrateCard.svelte';
	import CodeWindow from '$lib/components/CodeWindow.svelte';
	import UsageTabs from '$lib/components/UsageTabs.svelte';
	import ArchDiagram from '$lib/components/ArchDiagram.svelte';
	import ArrowIcon from '$lib/components/ArrowIcon.svelte';
	import {
		hero,
		proof,
		features,
		crates,
		usage,
		architecture,
		consumers,
		versioning,
		start
	} from '$lib/data';

	const description =
		'gateway is a provider-agnostic LLM inference routing engine for Rust — fallback chains, per-endpoint circuit breaker, budget management and request tracing across ~16 cloud providers plus in-process local models, behind one trait-based config.';

	// The versioning panel flips to the opposite mode of the page (mockup detail).
	const invMode = $derived(vibe.mode === 'dark' ? 'light' : 'dark');
</script>

<Seo title="gateway — LLM inference routing engine for Rust" {description} />

<a id="top"></a>

<!-- HERO -->
<section class="relative overflow-hidden">
	<div class="pointer-events-none absolute inset-0 bg-grid mask-fade-b opacity-50"></div>
	<div
		class="relative mx-auto flex max-w-content flex-col items-center gap-6 px-6 pb-16 pt-20 text-center lg:pt-28"
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
			<CodeWindow filename={hero.code.filename} code={hero.code.source} />
		</div>
	</div>
</section>

<!-- PROOF STRIP -->
<div class="border-y border-paper-edge bg-paper-soft">
	<div
		class="mx-auto flex max-w-content flex-wrap items-center gap-x-6 gap-y-2 px-6 py-4 font-mono text-sm text-ink-soft"
	>
		<span class="font-medium text-ink-mute">{proof.label}</span>
		{#each proof.providers as p (p)}
			<span>{p}</span>
		{/each}
	</div>
</div>

<!-- FEATURES -->
<section id="features" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={features.eyebrow} title={features.title} lede={features.lede} />
		<div class="mt-12 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
			{#each features.items as f (f.tag)}
				<FeatureCard tag={f.tag} title={f.title} body={f.body} />
			{/each}
		</div>
	</div>
</section>

<!-- CRATES -->
<section id="crates" class="grid-section border-y border-paper-edge bg-paper-soft">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={crates.eyebrow} title={crates.title} />
		<div class="mt-12 grid gap-6 md:grid-cols-2">
			{#each crates.items as c (c.name)}
				<CrateCard crate={c} />
			{/each}
		</div>
	</div>
</section>

<!-- USAGE -->
<section id="usage" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={usage.eyebrow} title={usage.title} lede={usage.lede} />
		<div class="mt-10">
			<UsageTabs tabs={usage.tabs} />
			<p class="mt-4 px-1 font-mono text-sm text-ink-soft">{usage.note}</p>
		</div>
	</div>
</section>

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

<!-- CONSUMERS -->
<section id="consumers" class="grid-section">
	<div class="mx-auto max-w-content px-6 py-section">
		<SectionHead eyebrow={consumers.eyebrow} title={consumers.title} lede={consumers.lede} />
		<div class="mt-12 grid gap-6 sm:grid-cols-2">
			{#each consumers.items as c (c.name)}
				<a
					href={c.repo}
					class="flex items-center gap-4 rounded-xl border border-paper-edge bg-paper-mute p-7 transition-colors hover:border-accent"
				>
					<span
						class="grid h-11 w-11 shrink-0 place-items-center rounded-lg border border-accent-line bg-accent-soft font-mono text-xl font-semibold text-primary"
					>
						{c.glyph}
					</span>
					<div>
						<div class="font-display font-semibold text-lg text-ink">{c.name}</div>
						<div class="font-mono text-sm text-ink-soft">
							{c.repo.replace('https://', '')}
						</div>
					</div>
				</a>
			{/each}
		</div>
	</div>
</section>

<!-- VERSIONING (inverted panel) -->
<section id="versioning" data-mode={invMode} class="border-y border-paper-edge bg-paper">
	<div
		class="mx-auto grid max-w-content items-center gap-10 px-6 py-section lg:grid-cols-2 lg:gap-14"
	>
		<div class="flex flex-col gap-4">
			<Eyebrow>{versioning.eyebrow}</Eyebrow>
			<h2 class="font-display font-semibold text-h2 text-ink text-balance">{versioning.title}</h2>
			<p class="max-w-xl text-lg text-ink-mute text-pretty">{versioning.lede}</p>
		</div>
		<div class="flex flex-col gap-3">
			{#each versioning.steps as s (s.n)}
				<div
					class="flex items-start gap-3 rounded-lg border border-paper-edge bg-paper-mute px-5 py-4"
				>
					<span class="font-mono text-sm text-primary">{s.n}</span>
					<div>
						<div class="font-display font-semibold text-ink">{s.title}</div>
						<div class="mt-1 font-mono text-sm text-ink-soft">{s.note}</div>
					</div>
				</div>
			{/each}
		</div>
	</div>
</section>

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
