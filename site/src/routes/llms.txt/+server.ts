import { pages } from '$lib/docs';
import { SITE_URL } from '$lib/seo';

export const prerender = true;

// The llms.txt convention: a curated, plain-text index of the docs for LLM agents.
// Charset is set explicitly so em-dashes don't render as mojibake on static hosts.
export function GET() {
	const lines = [
		'# gateway',
		'',
		'> Provider-agnostic multimodal inference routing engine for Rust — routes chat,',
		'> embeddings, image, video and speech across ~16 cloud providers plus in-process',
		'> local models. Fallback chains, per-endpoint circuit breaker, budget management,',
		'> multi-model consensus/panels, request tracing and BYOK credentials (API key or',
		'> OAuth/bearer), behind one trait-based config.',
		'',
		'## Docs',
		''
	];
	for (const p of pages) {
		lines.push(`- [${p.title}](${SITE_URL}/docs/${p.slug}): ${p.description}`);
	}
	lines.push(
		'',
		'## Agent skill',
		'',
		`- [using-gateway skill](${SITE_URL}/skills/using-gateway/SKILL.md): drop-in guide to add + use the crates in your repo`,
		`- Install: \`curl -fsSL ${SITE_URL}/skills/install.sh | sh\` (writes .claude/skills/using-gateway/SKILL.md)`,
		'',
		'## Full text',
		'',
		`- [All docs, concatenated](${SITE_URL}/llms-full.txt)`,
		`- [Source repository](https://github.com/sensei-hq/gateway)`,
		''
	);
	return new Response(lines.join('\n'), {
		headers: { 'content-type': 'text/plain; charset=utf-8' }
	});
}
