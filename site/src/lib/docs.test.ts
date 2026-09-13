import { describe, it, expect } from 'vitest';
import { marked } from 'marked';
import { addHeadingIds } from './docs';

marked.setOptions({ gfm: true });

/** The id a heading actually gets, driven through the same marked pipeline the
 *  page build uses — so these assert on real rendered HTML, not a hand-written
 *  approximation of it. */
function idFor(markdown: string): string {
	const html = addHeadingIds(marked.parse(markdown) as string);
	return (html.match(/id="([^"]*)"/) ?? [])[1] ?? '';
}

describe('addHeadingIds', () => {
	it('slugs a plain heading', () => {
		expect(idFor('## Plain heading')).toBe('plain-heading');
	});

	// marked escapes an apostrophe to &#39;. Left undecoded it contributes its
	// own numeric name to the anchor, which is how a "don't" heading published
	// as #what-you-don-39-t-... in a sibling project.
	it('does not leak an escaped apostrophe into the id', () => {
		expect(idFor("## What you don't configure")).toBe('what-you-don-t-configure');
	});

	// The docs write `<type>` / `<addr>` style placeholders; marked escapes those
	// to &lt;…&gt; inside <code>.
	it('does not leak HTML entity names into the id', () => {
		expect(idFor('## The `<type>` field')).toBe('the-type-field');
	});

	// Guards the opposite error: tags are markup, not word separators, so
	// stripping them must not split a word that inline emphasis ran through.
	it('does not split a word that inline markup runs through', () => {
		expect(idFor('## mid**dle**')).toBe('middle');
	});

	it('keeps the heading content untouched — only the id is derived', () => {
		const html = addHeadingIds(marked.parse('## The `<type>` field') as string);
		expect(html).toContain('<code>&lt;type&gt;</code>');
	});
});
