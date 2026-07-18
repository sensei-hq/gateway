import { pages } from '$lib/docs';

export const prerender = true;

// The full docs corpus as one plain-text file — the whole set for an LLM agent to
// ingest in a single fetch. Each doc's raw markdown, in reading order.
export function GET() {
	const body = pages.map((p) => p.raw.trimEnd()).join('\n\n\n---\n\n\n');
	return new Response(body + '\n', {
		headers: { 'content-type': 'text/plain; charset=utf-8' }
	});
}
