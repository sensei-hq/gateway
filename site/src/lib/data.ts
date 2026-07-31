/* gateway website — content data, ported from the design mockup (Gateway Site.dc.html).
   The release version is sourced from package.json (single source of truth), so the
   site follows `make bump` automatically and never drifts on a version change. */
import { version } from '../../package.json';

const REPO = 'https://github.com/sensei-hq/gateway';

/** Current release, from package.json — e.g. "0.4.0". */
export const VERSION = version;
/** Git-tag form consumers pin — e.g. "v0.4.0". */
export const TAG = `v${version}`;

export const brand = {
	name: 'gateway',
	full: 'multimodal inference routing',
	tagline: 'Provider-agnostic multimodal inference routing engine for Rust — chat, embeddings, image, video and speech. Fallback chains, circuit breaker, budget, multi-model consensus — cloud and local.',
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
# One dependency. Cloud is on by default; opt into local models with features.
gateway = { package = "sensei-gateway", git = "https://github.com/sensei-hq/gateway", tag = "${TAG}", features = ["local", "local-fastembed"] }`;

export const hero = {
	badge: ['Rust', 'Multimodal inference routing'],
	title: 'One routing engine for every model and modality.',
	lede: 'Fallback chains, circuit breakers and budget management across ~16 cloud providers — chat, embeddings, image, video and speech — plus in-process local models, behind one trait-based routing config.',
	primaryCta: { label: 'Get started guide', href: '/docs/quickstart' },
	secondaryCta: { label: 'View on GitHub', href: REPO },
	code: { filename: 'Cargo.toml', source: INSTALL }
};

export const proof = {
	label: 'Provider-agnostic:',
	providers: ['openai', 'anthropic', 'gemini', 'bedrock', 'grok', 'together', 'fal', 'flux', 'stability', 'runway', 'kling', 'huggingface', 'ollama', '+ local · llama.cpp · onnx · kokoro']
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
			tag: 'modalities',
			title: 'Chat, image, video & speech',
			body: 'One capability-trait surface routes text and embeddings plus image, video and speech generation — across ~16 cloud providers and in-process local models.'
		},
		{
			tag: 'consensus',
			title: 'Consensus & model panels',
			body: 'Fan a prompt across family-distinct models in parallel, or run a debate → synthesize → judge consensus workflow — in a single call.'
		},
		{
			tag: 'workflows',
			title: 'Purpose workflows',
			body: "Declare reusable multi-step pipelines that thread each step's output into the next and pick a model by tier, not by name."
		},
		{
			tag: 'tools',
			title: 'Provider-neutral tool calling',
			body: "Define tools once; the gateway translates to each provider's wire format and returns structured tool calls for you to run."
		},
		{
			tag: 'credentials',
			title: 'BYOK credentials & quotas',
			body: 'A separate vault seals API keys and OAuth/bearer tokens with envelope encryption; per-tier subscription quotas stop overspend before a request goes out.'
		},
		{
			tag: 'tracing',
			title: 'Streaming, tracing & your store',
			body: 'Stream tokens as they arrive, read an attempt-by-attempt trace, and plug in your own GatewayStore — gateway ships no DB.'
		}
	] as Feature[]
};

export type Crate = { name: string; version: string; body: string; chips: string[]; note?: string };

export const crates = {
	eyebrow: 'Four crates',
	title: 'Cloud and local, one config.',
	items: [
		{
			name: 'gateway',
			version: TAG,
			body: 'Provider-agnostic routing engine. Capability-trait adapters, named fallback chains, per-endpoint circuit breaker, budget filtering, multi-model consensus/panels and request tracing — with a store trait for persistence and quotas.',
			chips: ['reqwest', 'rustls', 'tokio']
		},
		{
			name: 'local-providers',
			version: TAG,
			body: 'In-process inference adapters — llama.cpp, fastembed, ONNX Runtime, and Kokoro text-to-speech. The same capability traits as the cloud adapters, so local and cloud models compose in one routing config.',
			chips: ['llama-cpp', 'fastembed', 'ort', 'kokoro'],
			note: 'feature-gated'
		},
		{
			name: 'local-engine',
			version: TAG,
			body: 'The local model engine: resolvers that map a model id to on-disk bytes (managed / Ollama / external) plus optional Hugging Face model pull.',
			chips: ['hf-download'],
			note: 'feature-gated'
		},
		{
			name: 'vault',
			version: TAG,
			body: 'Bring-your-own-key credential vault: API keys and OAuth/bearer tokens sealed with AES-256-GCM envelope encryption, per-tenant caching, Postgres/Supabase backing.',
			chips: ['aes-gcm', 'oauth', 'postgres'],
			note: 'optional'
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
			code: `# consumer workspace root — keep dev-only. Patching the one crate is enough;
# its sibling crates resolve as path deps of the local checkout.
[patch."https://github.com/sensei-hq/gateway"]
sensei-gateway = { path = "../gateway/crates/gateway" }`
		},
		{
			id: 'features',
			label: 'Features',
			code: `# sensei-gateway features
cloud              # cloud provider adapters (default)
local              # local engine: resolvers + provisioning supervisor
local-hf-download  # pull GGUF/ONNX models from the Hugging Face Hub
local-llama-cpp    # GGUF generation/embedding via llama.cpp
local-fastembed    # lightweight ONNX embeddings
local-ort          # ONNX Runtime (CPU)
local-kokoro       # in-process Kokoro text-to-speech`
		}
	] as UsageTab[]
};

export type ArchNode = { title: string; sub: string };
export type ArchDiagramData = {
	app: ArchNode;
	engine: { title: string; sub: string; caps: string[]; notes: string[] };
	adapter: { line1: string; line2: string; sub: string };
	cloud: { title: string; sub: string; pills: string[] };
	local: { title: string; sub: string; pills: string[] };
};

export const architecture = {
	eyebrow: 'Architecture',
	title: 'One adapter surface, many backends.',
	caption:
		'a small set of capability traits — chat, embeddings, image, video and speech, across cloud and local backends in one routing config',
	diagram: {
		app: { title: 'Your app', sub: 'route(model)' },
		engine: {
			title: 'gateway',
			sub: 'routing engine',
			caps: ['fallback', 'circuit breaker', 'budget', 'tracing'],
			notes: ['no DB · GatewayStore trait', 'reqwest · rustls · tokio']
		},
		adapter: { line1: 'Inference', line2: 'Adapter', sub: 'capability traits' },
		cloud: {
			title: 'Cloud providers',
			sub: '~16 adapters',
			pills: ['openai', 'anthropic', 'fal', 'flux', 'runway']
		},
		local: { title: 'Local engines', sub: 'in-process', pills: ['llama.cpp', 'onnx', 'kokoro'] }
	} as ArchDiagramData
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
	lede: `This repo versions independently of its consumers. Releases are semver tags — the workspace crates currently share ${TAG}.`,
	steps: [
		{ n: '01', title: 'Tag with semver', note: 'vMAJOR.MINOR.PATCH' },
		{ n: '02', title: 'Consumer pins the tag', note: `git dependency, tag = "${TAG}"` },
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
				{ label: 'local-providers', href: REPO },
				{ label: 'local-engine', href: REPO },
					{ label: 'vault', href: REPO }
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
