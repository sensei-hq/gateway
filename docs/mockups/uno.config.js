/* gateway design system - single source of truth.
 * Consumed by the design (browser runtime) and by the app repo (build) unchanged.
 * Semantic token names only. Never a descriptive name, never a literal in markup.
 * NOTE: the preflight is assembled from an array of lines on purpose - a stray
 * backtick in a template literal here would silently kill the whole file.
 */
(function () {
  var preflight = ["*,::before,::after{box-sizing:border-box;border-width:0;border-style:solid;border-color:var(--c-line)}","","/* --- theme axis: accent (hue only, so every theme stays in gamut) --- */",":root,[data-accent=\"blue\"]{--accent-c:.185;--accent-h:259}","[data-accent=\"violet\"]{--accent-c:.185;--accent-h:296}","[data-accent=\"teal\"]{--accent-c:.125;--accent-h:203}","","/* --- theme axis: light (default) MUST come before dark: equal specificity, source order wins --- */",":root,[data-theme=\"light\"]{","--accent-l:.61;","--c-bg:#fbfbfd;--c-surface:#ffffff;--c-panel:#fbfbfd;--c-stripe:#f4f5f8;--c-chip:#eef0f4;--c-nav:rgba(251,251,253,.82);","--c-line:rgba(18,20,26,.09);--c-line-strong:rgba(18,20,26,.14);--c-hairline:rgba(18,20,26,.07);","--c-fg:#12141a;--c-muted:#565c6b;--c-faint:#8b91a0;","--c-on-action:#ffffff;--c-on-accent:#ffffff;","--c-code-bg:#f6f8fa;--c-code-bar:#eef1f4;--c-code-line:#e3e6ea;--c-code-fg:#1f2328;--c-code-faint:#8b91a0;","--c-selection:#cfe0ff;","--shadow-frame:0 12px 40px -24px rgba(18,20,26,.28);","}","[data-theme=\"dark\"]{","--accent-l:.72;","--c-bg:#0b0d11;--c-surface:#12151b;--c-panel:#0f1319;--c-stripe:rgba(255,255,255,.045);--c-chip:rgba(255,255,255,.06);--c-nav:rgba(11,13,17,.85);","--c-line:rgba(255,255,255,.10);--c-line-strong:rgba(255,255,255,.18);--c-hairline:rgba(255,255,255,.08);","--c-fg:#eef1f5;--c-muted:#9aa4b2;--c-faint:#6b7280;","--c-on-action:#080a0e;--c-on-accent:#080a0e;","--c-code-bg:#0d1117;--c-code-bar:#161b22;--c-code-line:#1c2230;--c-code-fg:#e6edf3;--c-code-faint:#7d8698;","--c-selection:rgba(120,170,255,.35);","--shadow-frame:0 24px 60px -30px rgba(0,0,0,.6);","}","","/* --- theme axis: density --- */",":root,[data-density=\"default\"]{--section-y:5.75rem}","[data-density=\"compact\"]{--section-y:3.25rem}","","/* derived tokens are recomputed on every themed subtree, so a nested","   [data-theme] (the inverted versioning panel) re-derives its own accent */",":root,[data-theme],[data-accent],[data-density]{","--c-accent:oklch(var(--accent-l) var(--accent-c) var(--accent-h));","--c-accent-strong:oklch(calc(var(--accent-l) - .07) var(--accent-c) var(--accent-h));","--c-accent-wash:oklch(var(--accent-l) var(--accent-c) var(--accent-h)/.12);","--c-accent-line:oklch(var(--accent-l) var(--accent-c) var(--accent-h)/.32);","--c-action:var(--c-accent);--c-action-hover:var(--c-accent-strong);","}","","/* --- base reset. zero-specificity, or these outrank utilities --- */","body{margin:0;background:var(--c-bg);color:var(--c-fg);font-family:'IBM Plex Sans',system-ui,sans-serif;-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}",":where(a){color:inherit;text-decoration:none}",":where(h1,h2,h3,h4,p,pre,figure){margin:0}",":where(pre){font-family:'JetBrains Mono',ui-monospace,monospace}",":where(button){font:inherit;color:inherit;background:none;padding:0;cursor:pointer;-webkit-appearance:none;appearance:none}",":where(svg){display:block}","::selection{background:var(--c-selection)}","","@keyframes gw-flow{to{stroke-dashoffset:-30}}","@media (prefers-reduced-motion:reduce){*,::before,::after{animation-duration:.01ms !important;animation-iteration-count:1 !important;transition-duration:.01ms !important}}","","/* RUNTIME-INTEGRATION ONLY (no-op in the app build): the design host wraps each","   mounted component in a div, which would break sticky and full-bleed bands. */",".sc-host{display:contents}"].join('\n');

  /* exact shipped weights - fonts.css loads these and nothing else */
  var fonts = [
    { pkg: '@fontsource/space-grotesk', family: 'Space Grotesk', token: 'font-display', weights: [500, 600] },
    { pkg: '@fontsource/ibm-plex-sans',  family: 'IBM Plex Sans',  token: 'font-sans',    weights: [400, 500, 600] },
    { pkg: '@fontsource/jetbrains-mono', family: 'JetBrains Mono', token: 'font-mono',    weights: [400, 500, 600] }
  ];

  var theme = {
    breakpoints: { sm: '640px', md: '900px', lg: '1140px' },

    colors: {
      bg: 'var(--c-bg)',
      surface: 'var(--c-surface)',
      panel: 'var(--c-panel)',
      stripe: 'var(--c-stripe)',
      chip: 'var(--c-chip)',
      nav: 'var(--c-nav)',
      fg: 'var(--c-fg)',
      muted: 'var(--c-muted)',
      faint: 'var(--c-faint)',
      line: { DEFAULT: 'var(--c-line)', strong: 'var(--c-line-strong)', hair: 'var(--c-hairline)' },
      accent: { DEFAULT: 'var(--c-accent)', strong: 'var(--c-accent-strong)', wash: 'var(--c-accent-wash)', line: 'var(--c-accent-line)' },
      action: { DEFAULT: 'var(--c-action)', hover: 'var(--c-action-hover)' },
      'on-action': 'var(--c-on-action)',
      'on-accent': 'var(--c-on-accent)',
      code: { bg: 'var(--c-code-bg)', bar: 'var(--c-code-bar)', line: 'var(--c-code-line)', fg: 'var(--c-code-fg)', faint: 'var(--c-code-faint)' },
      dot: { red: '#ff5f57', amber: '#febc2e', green: '#28c840' }
    },

    fontFamily: {
      display: "'Space Grotesk',system-ui,sans-serif",
      sans: "'IBM Plex Sans',system-ui,sans-serif",
      mono: "'JetBrains Mono',ui-monospace,monospace"
    },
    fontWeight: { normal: '400', medium: '500', semibold: '600' },

    /* named steps, line-height baked in. no raw rem in markup. */
    fontSize: {
      micro:   ['0.75rem', '1.4'],
      eyebrow: ['0.78rem', '1.1'],
      caption: ['0.8125rem', '1.5'],
      code:    ['0.875rem', '1.85'],
      sub:     ['0.9375rem', '1.5'],
      body:    ['1rem', '1.6'],
      lede:    ['1.125rem', '1.55'],
      title:   ['1.25rem', '1.35'],
      subhead: ['1.5rem', '1.25'],
      head:    ['2.375rem', '1.1'],
      hero:    ['3rem', '1.06'],
      display: ['4rem', '1.02']
    },
    letterSpacing: { display: '-0.03em', head: '-0.02em', title: '-0.01em', brand: '-0.01em', eyebrow: '0.04em' },
    borderRadius: { 'control-sm': '0.5rem', control: '0.625rem', chip: '0.375rem', card: '1rem', pill: '100px' },
    /* only what the default scale has no answer for: the shell width and ch-based measures */
    maxWidth: { shell: '71.25rem', measure: '64ch', lede: '56ch', short: '48ch', hero: '16ch', cta: '18ch' },
    /* the default spacing/grid scale is used as-is. one addition, because a variable is a
       legal token value: the density axis drives section rhythm with no extra classes */
    spacing: { section: 'var(--section-y)' },
    boxShadow: {
      action: '0 4px 16px oklch(var(--accent-l) var(--accent-c) var(--accent-h)/.3)',
      brand: '0 2px 8px oklch(var(--accent-l) var(--accent-c) var(--accent-h)/.35)',
      lift: '0 8px 30px -16px oklch(var(--accent-l) var(--accent-c) var(--accent-h)/.4)',
      frame: 'var(--shadow-frame)'
    }
  };

  /* rule of three: 3+ utilities repeated on more than two elements */
  var shortcuts = {
    shell: 'w-full max-w-shell mx-auto px-8 lt-md:px-5',
    band: 'bg-surface border-y border-line-hair',
    card: 'bg-surface border border-line rounded-card',
    panel: 'bg-panel border border-line rounded-card',
    'card-lift': 'transition-all duration-150 hover:border-accent-line hover:shadow-lift',

    h1: 'font-display font-semibold text-display tracking-display lt-md:text-head',
    h2: 'font-display font-semibold text-head tracking-head lt-sm:text-subhead',
    h3: 'font-display font-semibold text-title tracking-title',
    h4: 'font-display font-semibold text-body tracking-title',
    eyebrow: 'font-mono text-eyebrow font-semibold tracking-eyebrow uppercase text-accent',
    lede: 'text-lede text-muted',
    meta: 'font-mono text-caption text-faint',

    btn: 'inline-flex items-center justify-center gap-2 font-sans font-semibold whitespace-nowrap cursor-pointer select-none transition-colors duration-150 focus-visible:[outline:2px_solid_var(--c-accent)] focus-visible:[outline-offset:2px]',
    'btn-primary': 'btn bg-action text-on-action shadow-action hover:bg-action-hover',
    'btn-secondary': 'btn bg-surface text-fg border border-line-strong hover:border-faint',
    'btn-ghost': 'btn bg-transparent text-muted hover:text-fg',
    'btn-sm': 'text-sub px-4 py-2.5 rounded-control min-h-10',
    'btn-md': 'text-body px-5.5 py-3.5 rounded-control min-h-12',
    'btn-lg': 'text-body px-6.5 py-4 rounded-control min-h-13',
    'icon-btn': 'inline-flex items-center justify-center w-11 h-11 rounded-control border border-line-strong text-muted transition-colors duration-150 hover:text-fg',

    chip: 'inline-flex items-center font-mono text-micro whitespace-nowrap px-2.5 py-1 rounded-chip bg-chip text-muted',
    'chip-accent': 'inline-flex items-center font-mono text-micro whitespace-nowrap px-2.5 py-0.5 rounded-pill bg-accent-wash text-accent border border-accent-line',
    frame: 'rounded-card overflow-hidden border border-code-line shadow-frame',
    'nav-link': 'text-sub font-medium text-muted transition-colors duration-150 hover:text-fg',
    dot: 'w-2.5 h-2.5 rounded-pill shrink-0'
  };

  var config = {
    theme: theme,
    shortcuts: shortcuts,
    rules: [],
    fonts: fonts,
    preflights: [{ getCSS: function () { return preflight; } }]
  };

  if (typeof window !== 'undefined') window.__unocss = config;
  if (typeof module !== 'undefined' && module.exports) module.exports = config;
})();
