<script lang="ts">
	// gateway brand mark — the gateway glyph (gateway.svg: a shield at 0.5 opacity
	// + a solid isometric cube). Recolours via currentColor while preserving the
	// per-path shading. Variants:
	//   on-blue — white glyph on the blue accent square (default; app-icon)
	//   white   — white glyph, transparent (for dark/coloured surfaces)
	//   black   — ink glyph, transparent (for light surfaces / print)
	let {
		class: className = 'h-8 w-8',
		variant = 'on-blue'
	}: { class?: string; variant?: 'on-blue' | 'white' | 'black' } = $props();

	const glyphColor = $derived(variant === 'black' ? '#12141a' : '#ffffff');
</script>

{#snippet glyph()}
	<path
		opacity="0.5"
		fill="currentColor"
		d="M12.308 2.98221C11.9441 2.85906 11.5498 2.85906 11.186 2.98221L4.40995 5.27721C4.18925 5.3489 3.99432 5.48356 3.84916 5.66461C3.704 5.84565 3.61495 6.06519 3.59295 6.29621C3.25595 10.5222 3.84095 13.6472 5.22995 16.0082C6.57395 18.2932 8.72395 19.9542 11.747 21.1922C14.773 19.9542 16.925 18.2932 18.269 16.0082C19.659 13.6482 20.244 10.5222 19.907 6.29621C19.885 6.06519 19.7959 5.84565 19.6507 5.66461C19.5056 5.48356 19.3107 5.3489 19.09 5.27721L12.308 2.98221ZM10.705 1.56221C11.3807 1.33312 12.113 1.33277 12.789 1.56121L19.571 3.85621C20.0691 4.02211 20.5078 4.33018 20.833 4.74244C21.1582 5.15469 21.3556 5.6531 21.401 6.17621C21.754 10.5862 21.161 14.0522 19.562 16.7692C17.958 19.4942 15.401 21.3672 12.022 22.6982C11.8452 22.7679 11.6487 22.7679 11.472 22.6982C8.09595 21.3672 5.53995 19.4942 3.93695 16.7692C2.33895 14.0522 1.74595 10.5852 2.09795 6.17621C2.14327 5.6531 2.3407 5.15469 2.66588 4.74244C2.99106 4.33018 3.42978 4.02211 3.92795 3.85621L10.705 1.56221Z"
	/>
	<path
		fill="currentColor"
		d="M11.6667 10.9244L8 9.38215V12.8177C8.00193 12.961 8.0448 13.1007 8.12355 13.2204C8.2023 13.3401 8.31364 13.4348 8.44444 13.4933L11.64 15H11.6667V10.9244ZM12 10.3333L15.7822 8.74658C15.7187 8.67569 15.6417 8.61825 15.5556 8.57769L12.3556 7.0799C12.2444 7.02729 12.123 7 12 7C11.877 7 11.7556 7.02729 11.6444 7.0799L8.44444 8.57769C8.35835 8.61825 8.28127 8.67569 8.21778 8.74658L12 10.3333ZM12.3333 10.9244V15H12.3556L15.5556 13.4933C15.6857 13.4351 15.7966 13.3412 15.8753 13.2223C15.954 13.1035 15.9972 12.9647 16 12.8222V9.38215L12.3333 10.9244Z"
	/>
{/snippet}

{#if variant === 'on-blue'}
	<svg
		viewBox="0 0 32 32"
		class={className}
		style="color:#ffffff"
		xmlns="http://www.w3.org/2000/svg"
		aria-hidden="true"
	>
		<rect width="32" height="32" rx="8" fill="#2f7ff2" />
		<g transform="translate(4 4)">
			{@render glyph()}
		</g>
	</svg>
{:else}
	<svg
		viewBox="0 0 24 24"
		class={className}
		style="color:{glyphColor}"
		xmlns="http://www.w3.org/2000/svg"
		aria-hidden="true"
	>
		{@render glyph()}
	</svg>
{/if}
