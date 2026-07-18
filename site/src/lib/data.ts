/* gateway website — content data, ported from the design mockup (Gateway Site.dc.html),
   with facts updated to the current release (v0.3.x, capability-trait adapters). */

const REPO = 'https://github.com/sensei-hq/gateway';

export const brand = {
	name: 'gateway',
	full: 'LLM inference routing',
	tagline: 'Shared LLM inference routing engine for Rust. Fallback chains, circuit breaker, budget management — cloud and local.',
	repo: REPO
};

export const nav = {
	links: [
		{ label: 'Features', href: '/#features' },
		{ label: 'Crates', href: '/#crates' },
		{ label: 'Usage', href: '/#usage' },
		{ label: 'Architecture', href: '/#architecture' },
		{ label: 'Docs', href: '/docs' },
		{ label: 'GitHub', href: REPO }
	],
	cta: { label: 'Get started', href: '/#start' }
};

const INSTALL = `[dependencies]
gateway          = { git = "https://github.com/sensei-hq/gateway", tag = "v0.3.1" }
gateway-embedded = { git = "https://github.com/sensei-hq/gateway", tag = "v0.3.1", features = ["fastembed"] }`;

export const hero = {
	badge: ['Rust', 'LLM inference routing'],
	title: 'One routing engine for every model provider.',
	lede: 'Fallback chains, circuit breakers and budget management across ~16 cloud providers — plus in-process local models — behind one trait-based routing config.',
	primaryCta: { label: 'Get started guide', href: '/docs/quickstart' },
	secondaryCta: { label: 'View on GitHub', href: REPO },
	code: { filename: 'Cargo.toml', source: INSTALL }
};

export const proof = {
	label: 'Provider-agnostic:',
	providers: ['openai', 'anthropic', 'gemini', 'bedrock', 'grok', 'together', 'huggingface', 'ollama', '+ local · llama.cpp · onnx']
};

export type Feature = { tag: string; title: string; body: string };

export const features = {
	eyebrow: 'The routing engine',
	title: 'Everything a request needs to reach a healthy model.',
	lede: 'No database of its own, no lock-in. HTTP via reqwest/rustls, async via tokio.',
	items: [
		{
			tag: 'fallback',
			title: 'Named fallback chains',
			body: 'Chain endpoints by name. When one fails or blows its budget, the next takes over — automatically, per request.'
		},
		{
			tag: 'circuit',
			title: 'Per-endpoint circuit breaker',
			body: "Trips on repeated failures and backs off, so a flaky provider can't drag the whole chain down with it."
		},
		{
			tag: 'budget',
			title: 'Budget filtering & metering',
			body: 'Filter candidates by cost before a request goes out, and record real per-call spend so burn-rate is queryable.'
		},
		{
			tag: 'adapters',
			title: '~16 cloud adapters + local',
			body: 'Capability-trait adapters for around sixteen cloud providers, plus in-process local models. Add your own by implementing a capability trait.'
		},
		{
			tag: 'tracing',
			title: 'Streaming & request tracing',
			body: 'Stream tokens as they arrive, and carry structured trace context through the pipeline — you see exactly which endpoint served each request.'
		},
		{
			tag: 'store',
			title: 'Bring your own store',
			body: 'A GatewayStore trait handles persistence and subscription quotas. Wire in whatever you already run — gateway ships no DB.'
		}
	] as Feature[]
};

export type Crate = { name: string; version: string; body: string; chips: string[]; note?: string };

export const crates = {
	eyebrow: 'Two crates',
	title: 'Cloud and local, one config.',
	items: [
		{
			name: 'gateway',
			version: 'v0.3.1',
			body: 'Provider-agnostic routing engine. Capability-trait adapters, named fallback chains, per-endpoint circuit breaker, budget filtering and request tracing — with a store trait for persistence and quotas.',
			chips: ['reqwest', 'rustls', 'tokio']
		},
		{
			name: 'gateway-embedded',
			version: 'v0.3.1',
			body: 'In-process inference adapters and an on-disk model registry. The same capability traits as the cloud adapters, so local and cloud models compose in one routing config.',
			chips: ['llama-cpp', 'fastembed', 'ort'],
			note: 'feature-gated'
		}
	] as Crate[]
};

export type UsageTab = { id: string; label: string; code: string };

export const usage = {
	eyebrow: 'Consuming it',
	title: 'Pin a tag. Lock the commit.',
	lede: "Add it as a git dependency on a tagged release. Cargo.lock in your binary pins the exact commit, so there's no silent drift between builds. Developing in-place? Clone next to your consumer and add a dev-only [patch] at the workspace root.",
	note: '// after editing locally: push, cut a new tag, bump the pinned tag in each consumer',
	tabs: [
		{ id: 'add', label: 'Cargo.toml', code: INSTALL },
		{
			id: 'patch',
			label: 'Local dev',
			code: `# consumer workspace root — keep dev-only
[patch."https://github.com/sensei-hq/gateway"]
gateway          = { path = "../gateway/crates/gateway" }
gateway-embedded = { path = "../gateway/crates/gateway-embedded" }`
		},
		{
			id: 'features',
			label: 'Embedded engines',
			code: `# gateway-embedded features (all off by default)
llama-cpp     # GGUF generation/embedding via llama.cpp
fastembed     # lightweight embeddings
ort           # ONNX Runtime (CPU)
hf-download   # pull GGUF/ONNX models from the Hugging Face Hub`
		}
	] as UsageTab[]
};

export const architecture = {
	eyebrow: 'Architecture',
	title: 'One adapter surface, many backends.',
	caption: 'a small set of capability traits — cloud and local backends compose in a single routing config'
};

export type Consumer = { name: string; glyph: string; repo: string };

export const consumers = {
	eyebrow: 'Shipped in production',
	title: 'Consumed by sibling projects.',
	lede: 'gateway is the shared inference layer under two projects in the sensei-hq org.',
	items: [
		{ name: 'sensei', glyph: 's', repo: 'https://github.com/sensei-hq/sensei' },
		{ name: 'strategos', glyph: 'σ', repo: 'https://github.com/sensei-hq/strategos' }
	] as Consumer[]
};

export type Step = { n: string; title: string; note: string };

export const versioning = {
	eyebrow: 'Versioning',
	title: 'Independent, semver, reproducible.',
	lede: 'This repo versions independently of its consumers. Releases are semver tags — both crates currently share v0.3.1.',
	steps: [
		{ n: '01', title: 'Tag with semver', note: 'vMAJOR.MINOR.PATCH' },
		{ n: '02', title: 'Consumer pins the tag', note: 'git dependency, tag = "v0.3.1"' },
		{ n: '03', title: 'Cargo.lock pins the commit', note: 'exact commit · no silent drift' }
	] as Step[]
};

export const start = {
	title: 'Start routing through gateway.',
	lede: 'Add the git dependency, name a fallback chain, and let the engine handle the rest.',
	primaryCta: { label: 'Get started guide', href: '/docs/quickstart' },
	secondaryCta: { label: 'Star on GitHub', href: REPO }
};

export const footer = {
	tagline: brand.tagline,
	columns: [
		{
			title: 'Crates',
			links: [
				{ label: 'gateway', href: REPO },
				{ label: 'gateway-embedded', href: REPO }
			]
		},
		{
			title: 'Resources',
			links: [
				{ label: 'Docs', href: '/docs' },
				{ label: 'llms.txt', href: '/llms.txt' },
				{ label: 'GitHub', href: REPO },
				{ label: 'Releases', href: `${REPO}/releases` }
			]
		}
	],
	legal: 'MIT licensed · sensei-hq'
};
