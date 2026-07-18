/**
 * Prebuild step: sync the LLM/usage guides into the site.
 *
 *   ../docs/llms/*.md → src/lib/content/docs/  (rendered at /docs/<slug>,
 *                       and served raw at /llms-full.txt)
 *
 * Keeps a single source of truth in docs/llms — the site never forks the content.
 */
import { mkdirSync, copyFileSync, readdirSync, existsSync, rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, '..', '..'); // gateway/
const src = join(repo, 'docs', 'llms'); // gateway/docs/llms
const dest = join(here, '..', 'src', 'lib', 'content', 'docs');

rmSync(dest, { recursive: true, force: true });
mkdirSync(dest, { recursive: true });

if (existsSync(src)) {
	for (const f of readdirSync(src).filter((n) => n.endsWith('.md'))) {
		copyFileSync(join(src, f), join(dest, f));
		console.log(`  copied docs/llms/${f}`);
	}
} else {
	console.warn(`  ! missing ${src} — /docs will be empty`);
}
console.log('docs sync complete.');
