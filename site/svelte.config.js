import adapterAuto from '@sveltejs/adapter-auto';
import adapterCloudflare from '@sveltejs/adapter-cloudflare';

// Adapter selection ("keep both" — Vercel + Cloudflare):
//   - On Cloudflare (the project sets CF_PAGES=1; Workers Builds also sets
//     WORKERS_CI) use @sveltejs/adapter-cloudflare explicitly. It emits
//     .svelte-kit/cloudflare/_worker.js, which the committed wrangler.jsonc
//     deploys. Selecting it here (instead of letting adapter-auto auto-install
//     it at build time) avoids the frozen-lockfile auto-install failure.
//   - Everywhere else (incl. Vercel, which sets VERCEL) adapter-auto picks the
//     right target. Every route is prerendered (src/routes/+layout.ts), so the
//     output is static.
const onCloudflare = !!process.env.CF_PAGES || !!process.env.WORKERS_CI;
const adapter = onCloudflare ? adapterCloudflare() : adapterAuto();

/** @type {import('@sveltejs/kit').Config} */
const config = {
	compilerOptions: {
		// Force runes mode for the project, except for libraries. Can be removed in svelte 6.
		runes: ({ filename }) => (filename.split(/[/\\]/).includes('node_modules') ? undefined : true)
	},
	kit: {
		adapter
	}
};

export default config;
