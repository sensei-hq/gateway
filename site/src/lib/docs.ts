import { marked } from 'marked';

// Raw markdown is synced from /docs/llms at prebuild (scripts/copy-docs.mjs).
// Rendered with marked + injected as HTML (build-time, trusted content).
const raws = import.meta.glob('./content/docs/*.md', {
	eager: true,
	query: '?raw',
	import: 'default'
});

const REPO_BLOB = 'https://github.com/sensei-hq/gateway/blob/develop';

marked.setOptions({ gfm: true });

// Curated order + display titles + slugs, keyed by the source filename (no .md).
// Files not listed fall back to filename slug / H1 title / order 99.
const META: Record<string, { slug: string; title: string; order: number }> = {
	README: { slug: 'overview', title: 'Overview', order: 0 },
	quickstart: { slug: 'quickstart', title: 'Quickstart', order: 1 },
	configuration: { slug: 'configuration', title: 'Configuration', order: 2 },
	recipes: { slug: 'recipes', title: 'Recipes', order: 3 },
	local: { slug: 'local', title: 'Local', order: 4 },
	'custom-adapters': { slug: 'custom-adapters', title: 'Custom adapters', order: 5 },
	upgrading: { slug: 'upgrading', title: 'Upgrading', order: 6 }
};

/** Map a bare guide filename (no dir, no .md) to its site slug, if it's a doc. */
function slugForFile(base: string): string | null {
	if (META[base]) return META[base].slug;
	// unknown same-dir guide → its own basename
	return /^[a-z0-9._-]+$/i.test(base) ? base : null;
}

/** Derive an anchor slug from a heading's rendered inner HTML.
 *
 *  `inner` is markup, not text. marked wraps inline code in `<code>` and escapes
 *  what the docs write — an apostrophe becomes `&#39;`, a `<type>` placeholder
 *  becomes `&lt;type&gt;`. Both have to go, but not the same way:
 *
 *    - Tags are removed outright, because inline emphasis can run through the
 *      middle of a word — `mid<strong>dle</strong>` is one word, and turning the
 *      tags into separators would slug it as `mid-dle`.
 *    - An entity reference stands in for punctuation, so it becomes a separator.
 *      Left alone it leaks its own name into the anchor: "don't" slugged as
 *      `don-39-t`, and `<type>` as `lt-type-gt`.
 *
 *  Entities are deliberately NOT decoded back to characters: that would put a
 *  literal `<` into a value about to be interpolated into an `id="…"` attribute.
 *  Mapping them straight to a separator keeps markup from ever re-entering the
 *  string, and the final `[^\w]` pass leaves only `[a-z0-9_-]`.
 */
function headingSlug(inner: string): string {
	return inner
		.replace(/<[^>]*>/g, '')
		.replace(/&(?:[a-z][a-z0-9]*|#\d+|#x[0-9a-f]+);/gi, '-')
		.toLowerCase()
		.trim()
		.replace(/[^\w]+/g, '-')
		.replace(/^-+|-+$/g, '');
}

// Give headings slug ids so in-page anchors resolve. Only the id is derived —
// `inner` is re-emitted exactly as marked rendered it.
export function addHeadingIds(html: string): string {
	return html.replace(
		/<(h[1-6])>(.*?)<\/\1>/g,
		(_full, tag, inner) => `<${tag} id="${headingSlug(inner)}">${inner}</${tag}>`
	);
}

// Rewrite markdown links: same-dir guide links (`quickstart.md#x`) → `/docs/<slug>#x`;
// any other repo-relative path (`docs/features/…`, `crates/…`) → the GitHub blob URL
// so it resolves instead of 404-ing on the site. External + anchor links untouched.
function rewriteLinks(html: string): string {
	return html.replace(/href="([^"]+)"/g, (full, href) => {
		if (/^(https?:)?\/\/|^#|^mailto:/i.test(href)) return full;
		const h = href.replace(/^\.\//, '');
		const bare = h.match(/^([a-z0-9._-]+)\.md(#[^"]*)?$/i);
		if (bare) {
			const slug = slugForFile(bare[1]);
			if (slug) return `href="/docs/${slug}${bare[2] ?? ''}"`;
		}
		// A repo-relative path (with a directory) → link out to GitHub.
		if (!h.startsWith('/')) return `href="${REPO_BLOB}/${h}"`;
		return full;
	});
}

export type DocPage = {
	slug: string;
	order: number;
	title: string;
	description: string;
	raw: string;
	html: string;
};

function h1Of(raw: string, fallback: string): string {
	const m = raw.match(/^#\s+(.+)$/m);
	return m ? m[1].trim() : fallback;
}

/** First prose paragraph after the H1, stripped to plain text + clamped — used as
 *  the page's meta/OG description. */
function descriptionFrom(raw: string, fallback: string): string {
	const body = raw.replace(/^#\s+.+$/m, '');
	for (const block of body.split(/\n\s*\n/)) {
		const t = block.trim();
		// Skip headings, fenced code, tables, blockquotes, list items, raw HTML —
		// but NOT prose that merely starts with an inline `code` span.
		if (!t || /^(#|```|\||>|<|[-*]\s|\d+\.\s)/.test(t)) continue;
		const text = t
			.replace(/`([^`]+)`/g, '$1')
			.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
			.replace(/\*+/g, '') // strip * emphasis, keep snake_case underscores
			.replace(/\s+/g, ' ')
			.trim();
		if (text.length < 20) continue;
		return text.length > 155 ? text.slice(0, 152).trimEnd() + '…' : text;
	}
	return fallback;
}

export const pages: DocPage[] = Object.entries(raws)
	.map(([path, raw]) => {
		const md = raw as string;
		const file = (path.split('/').pop() ?? '').replace('.md', '');
		const meta = META[file];
		const slug = meta?.slug ?? file;
		const title = meta?.title ?? h1Of(md, slug);
		return {
			slug,
			order: meta?.order ?? 99,
			title,
			description: descriptionFrom(md, `${title} — gateway documentation.`),
			raw: md,
			html: rewriteLinks(addHeadingIds(marked.parse(md) as string))
		};
	})
	.sort((a, b) => a.order - b.order);

export function findPage(slug: string): DocPage | undefined {
	return pages.find((p) => p.slug === slug);
}
