import { error } from '@sveltejs/kit';
import { pages, findPage } from '$lib/docs';

export const prerender = true;

// Enumerate slugs so the prerenderer emits one HTML file per doc page.
export function entries() {
	return pages.map((p) => ({ slug: p.slug }));
}

export function load({ params }: { params: { slug: string } }) {
	if (!findPage(params.slug)) error(404, `Docs page "${params.slug}" not found`);
	return { slug: params.slug };
}
