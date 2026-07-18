import { pages } from '$lib/docs';
import { SITE_URL } from '$lib/seo';

export const prerender = true;

// The llms.txt convention: a curated, plain-text index of the docs for LLM agents.
// Charset is set explicitly so em-dashes don't render as mojibake on static hosts.
export function GET() {
	const lines = [
		'# gateway',
		'',
		'> Shared LLM inference routing engine for Rust — fallback chains, per-endpoint',
		'> circuit breaker, budget management and request tracing across ~16 cloud',
		'> providers plus in-process local models, behind one trait-based config.',
		'',
		'## Docs',
		''
	];
	for (const p of pages) {
		lines.push(`- [${p.title}](${SITE_URL}/docs/${p.slug}): ${p.description}`);
	}
	lines.push(
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
