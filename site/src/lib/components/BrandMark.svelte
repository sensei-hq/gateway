<script lang="ts">
	// gateway brand mark — a blue rounded square with a monospace "g" (matches the mockup).
	let { class: className = 'h-8 w-8' }: { class?: string } = $props();
</script>

<svg viewBox="0 0 32 32" class={className} xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
	<rect width="32" height="32" rx="8" fill="#2f7ff2" />
	<text
		x="16"
		y="16"
		text-anchor="middle"
		dominant-baseline="central"
		font-family="'IBM Plex Mono', ui-monospace, monospace"
		font-size="18"
		font-weight="600"
		fill="#ffffff">g</text
	>
</svg>
