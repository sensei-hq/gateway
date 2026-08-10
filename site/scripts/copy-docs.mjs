/**
 * Prebuild step: sync repo content into the site.
 *
 *   ../docs/llms/*.md → src/lib/content/docs/  (rendered at /docs/<slug>,
 *                       and served raw at /llms-full.txt)
 *   ../docs/skills/**  → static/skills/         (served for download at
 *                       /skills/<name>/SKILL.md, installed into a consumer repo)
 *
 * Keeps a single source of truth in docs/ — the site never forks the content.
 */
import { mkdirSync, copyFileSync, readdirSync, existsSync, rmSync, cpSync } from 'node:fs';
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

// Sync agent skills into static/ so they're downloadable from the deployed site.
const skillsSrc = join(repo, 'docs', 'skills'); // gateway/docs/skills
const skillsDest = join(here, '..', 'static', 'skills');
rmSync(skillsDest, { recursive: true, force: true });
if (existsSync(skillsSrc)) {
	cpSync(skillsSrc, skillsDest, { recursive: true });
	console.log('  copied docs/skills → static/skills');
}

console.log('docs sync complete.');
