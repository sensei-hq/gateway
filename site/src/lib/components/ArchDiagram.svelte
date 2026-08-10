<script lang="ts">
	import { select } from 'd3-selection';
	import { vibe } from '@rokkit/states';
	import type { ArchDiagramData } from '$lib/data';

	let { data }: { data: ArchDiagramData } = $props();

	let host: HTMLDivElement;
	let probes: HTMLDivElement;

	const SG = "'Space Grotesk', sans-serif";
	const MONO = "'IBM Plex Mono', monospace";

	function readPalette() {
		// Each probe span carries a site token utility; read its resolved color.
		const q = (sel: string, prop: 'color' | 'backgroundColor' | 'borderTopColor') =>
			getComputedStyle(probes.querySelector(sel) as Element)[prop];
		return {
			fg: q('.text-ink', 'color'),
			muted: q('.text-ink-mute', 'color'),
			faint: q('.text-ink-soft', 'color'),
			accent: q('.text-primary', 'color'),
			accentWash: q('.bg-accent-soft', 'backgroundColor'),
			accentLine: q('.border-accent-line', 'borderTopColor'),
			surface: q('.bg-paper', 'backgroundColor'),
			line: q('.border-paper-edge', 'borderTopColor'),
			chip: q('.bg-paper-soft', 'backgroundColor')
		};
	}

	function draw() {
		if (!host || !probes) return;
		const reduce =
			typeof window !== 'undefined' &&
			window.matchMedia('(prefers-reduced-motion: reduce)').matches;
		const T = readPalette();
		const a = data;

		select(host).selectAll('*').remove();
		const svg = select(host)
			.append('svg')
			.attr('class', 'w-full h-auto overflow-visible')
			.attr('viewBox', '0 0 1080 440')
			.attr('preserveAspectRatio', 'xMidYMid meet')
			.attr('role', 'img')
			.attr(
				'aria-label',
				'gateway routes a model request through capability-trait adapters to cloud and local backends'
			);

		const mk = svg
			.append('defs')
			.append('marker')
			.attr('id', 'gw-arw')
			.attr('viewBox', '0 0 8 8')
			.attr('refX', 6.4)
			.attr('refY', 4)
			.attr('markerWidth', 6)
			.attr('markerHeight', 6)
			.attr('orient', 'auto-start-reverse');
		mk.append('path').attr('d', 'M0.5,0.5 L7.5,4 L0.5,7.5 Z').style('fill', T.faint);

		const linkG = svg.append('g');
		const curve = (x1: number, y1: number, x2: number, y2: number) => {
			const mx = (x1 + x2) / 2;
			return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
		};
		(
			[
				[172, 220, 246, 220],
				[582, 220, 636, 220],
				[772, 212, 826, 146],
				[772, 228, 826, 309]
			] as const
		).forEach((l) => {
			const d = curve(l[0], l[1], l[2], l[3]);
			linkG
				.append('path')
				.attr('d', d)
				.style('fill', 'none')
				.style('stroke', T.line)
				.style('stroke-width', 1.75)
				.style('stroke-linecap', 'round')
				.attr('marker-end', 'url(#gw-arw)');
			const flow = linkG
				.append('path')
				.attr('d', d)
				.style('fill', 'none')
				.style('stroke', T.accent)
				.style('stroke-width', 2)
				.style('stroke-linecap', 'round')
				.style('stroke-dasharray', '0.5 13')
				.style('opacity', 0.5);
			if (!reduce) flow.style('animation', 'gw-flow 1.5s linear infinite');
		});

		const cardRect = (g: any, x: number, y: number, w: number, h: number, accent: boolean) =>
			g
				.append('rect')
				.attr('x', x)
				.attr('y', y)
				.attr('width', w)
				.attr('height', h)
				.attr('rx', 16)
				.style('fill', accent ? T.accentWash : T.surface)
				.style('stroke', accent ? T.accentLine : T.line)
				.style('stroke-width', 1);
		const txt = (g: any, x: number, y: number, str: string, o: any = {}) =>
			g
				.append('text')
				.attr('x', x)
				.attr('y', y)
				.text(str)
				.style('font-family', o.mono ? MONO : SG)
				.style('font-size', `${o.size || 14}px`)
				.style('font-weight', o.weight || 600)
				.style('fill', o.fill || T.fg)
				.style('text-anchor', o.anchor || 'start')
				.style('letter-spacing', o.mono ? '0' : '-.01em');
		const pills = (g: any, labels: string[], x0: number, y0: number, maxX: number) => {
			let x = x0,
				y = y0;
			const h = 24,
				pad = 9,
				gap = 7;
			labels.forEach((l) => {
				const w = Math.round(l.length * 6.7) + pad * 2;
				if (x + w > maxX) {
					x = x0;
					y += h + 7;
				}
				g.append('rect')
					.attr('x', x)
					.attr('y', y)
					.attr('width', w)
					.attr('height', h)
					.attr('rx', 7)
					.style('fill', T.chip);
				g.append('text')
					.attr('x', x + pad)
					.attr('y', y + 16)
					.text(l)
					.style('font-family', MONO)
					.style('font-size', '11.5px')
					.style('fill', T.muted);
				x += w + gap;
			});
		};

		const app = svg.append('g');
		cardRect(app, 20, 170, 150, 100, false);
		txt(app, 95, 212, a.app.title, { size: 17, anchor: 'middle' });
		txt(app, 95, 236, a.app.sub, { mono: true, size: 12, weight: 500, fill: T.faint, anchor: 'middle' });

		const gw = svg.append('g');
		cardRect(gw, 250, 66, 330, 308, true);
		gw.append('circle').attr('cx', 300).attr('cy', 103).attr('r', 5).style('fill', T.accent);
		txt(gw, 315, 110, a.engine.title, { size: 23 });
		txt(gw, 315, 132, a.engine.sub, { mono: true, size: 12, weight: 500, fill: T.accent });
		gw
			.append('line')
			.attr('x1', 274)
			.attr('y1', 152)
			.attr('x2', 556)
			.attr('y2', 152)
			.style('stroke', T.accentLine)
			.style('stroke-width', 1);
		const capW = 262,
			capX = 284,
			capH = 27,
			capGap = 8,
			capY0 = 166;
		a.engine.caps.forEach((c, i) => {
			const y = capY0 + i * (capH + capGap);
			gw
				.append('rect')
				.attr('x', capX)
				.attr('y', y)
				.attr('width', capW)
				.attr('height', capH)
				.attr('rx', 9)
				.style('fill', T.surface)
				.style('stroke', T.line)
				.style('stroke-width', 1);
			gw
				.append('circle')
				.attr('cx', capX + 16)
				.attr('cy', y + capH / 2)
				.attr('r', 3.5)
				.style('fill', T.accent);
			txt(gw, capX + 30, y + capH / 2 + 4, c, { mono: true, size: 12, weight: 500, fill: T.muted });
		});
		gw
			.append('line')
			.attr('x1', 274)
			.attr('y1', 313)
			.attr('x2', 556)
			.attr('y2', 313)
			.style('stroke', T.accentLine)
			.style('stroke-width', 1);
		a.engine.notes.forEach((n, i) =>
			txt(gw, 415, 334 + i * 19, n, { mono: true, size: 11, weight: 400, fill: T.faint, anchor: 'middle' })
		);

		const ad = svg.append('g');
		cardRect(ad, 640, 160, 130, 120, false);
		txt(ad, 705, 205, a.adapter.line1, { size: 15, anchor: 'middle' });
		txt(ad, 705, 225, a.adapter.line2, { size: 15, anchor: 'middle' });
		txt(ad, 705, 248, a.adapter.sub, { mono: true, size: 11.5, weight: 500, fill: T.accent, anchor: 'middle' });

		const cl = svg.append('g');
		cardRect(cl, 830, 70, 230, 150, false);
		txt(cl, 852, 104, a.cloud.title, { size: 15 });
		txt(cl, 852, 124, a.cloud.sub, { mono: true, size: 11.5, weight: 500, fill: T.accent });
		pills(cl, a.cloud.pills, 852, 140, 1044);

		const lo = svg.append('g');
		cardRect(lo, 830, 250, 230, 150, false);
		txt(lo, 852, 284, a.local.title, { size: 15 });
		txt(lo, 852, 304, a.local.sub, { mono: true, size: 11.5, weight: 500, fill: T.accent });
		pills(lo, a.local.pills, 852, 320, 1044);
	}

	// Redraw on mount and whenever the theme mode flips (re-reads token colors).
	$effect(() => {
		void vibe.mode;
		draw();
	});
</script>

<!-- Hidden probes: carry the site token utilities so d3 can read resolved colors. -->
<div
	bind:this={probes}
	aria-hidden="true"
	style="position:absolute; width:0; height:0; overflow:hidden; visibility:hidden"
>
	<span class="text-ink"></span>
	<span class="text-ink-mute"></span>
	<span class="text-ink-soft"></span>
	<span class="text-primary"></span>
	<span class="bg-accent-soft"></span>
	<span class="border-accent-line"></span>
	<span class="bg-paper"></span>
	<span class="border-paper-edge"></span>
	<span class="bg-paper-soft"></span>
</div>

<div bind:this={host} class="w-full min-h-75"></div>

<style>
	/* -global- keeps the name un-hashed so the d3 JS (which applies
	   `animation: gw-flow …`) matches this rule; a scoped name would not. */
	@keyframes -global-gw-flow {
		to {
			stroke-dashoffset: -27;
		}
	}
</style>
