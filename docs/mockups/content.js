/* All copy and data. No copy lives in markup, so text edits never touch layout
 * and a translation/CMS swap is this one file. Each segment reads its own slice.
 */
(function () {
  var VERSION = 'v0.2.18';
  var REPO = 'https://github.com/sensei-hq/gateway';

  var content = {
    version: VERSION,
    repo: REPO,

    nav: {
      brand: 'gateway',
      mark: 'g',
      links: [
        { label: 'Features', href: '#features' },
        { label: 'Crates', href: '#crates' },
        { label: 'Usage', href: '#usage' },
        { label: 'Versioning', href: '#versioning' },
        { label: 'GitHub', href: REPO }
      ],
      cta: { label: 'Get started', href: '#start' }
    },

    hero: {
      badgeMono: 'Rust',
      badge: 'LLM inference routing',
      title: 'One routing engine for every model provider.',
      body: 'Fallback chains, circuit breakers and budget management across ~15 cloud providers - plus in-process local models - behind one trait-based routing config.',
      primary: { label: 'Get started guide', href: '#start' },
      secondary: { label: 'View on GitHub', href: REPO },
      filename: 'Cargo.toml',
      install: [
        '[dependencies]',
        'gateway          = { git = "' + REPO + '", tag = "' + VERSION + '" }',
        'gateway-embedded = { git = "' + REPO + '", tag = "' + VERSION + '", features = ["fastembed"] }'
      ].join('\n')
    },

    proof: {
      label: 'Provider-agnostic:',
      items: ['openai', 'anthropic', 'google', 'mistral', 'cohere', 'groq', 'bedrock', '+ local - llama.cpp - onnx']
    },

    features: {
      eyebrow: 'The routing engine',
      title: 'Everything a request needs to reach a healthy model.',
      body: 'No database of its own, no lock-in.',
      mono: 'reqwest - rustls - tokio',
      items: [
        { tag: 'fallback', title: 'Named fallback chains', body: 'Chain endpoints by name. When one fails or blows its budget, the next takes over - automatically, per request.' },
        { tag: 'circuit', title: 'Per-endpoint circuit breaker', body: 'Trips on repeated failures and backs off, so a flaky provider cannot drag the whole chain down with it.' },
        { tag: 'budget', title: 'Budget filtering', body: 'Filter candidates by cost before a request goes out. Routes stay inside their spend limits by design.' },
        { tag: 'adapters', title: '~15 cloud adapters', body: 'Trait-based adapters for around fifteen providers. Add your own by implementing a single trait.' },
        { tag: 'tracing', title: 'Request tracing', body: 'Every request carries structured trace context through the pipeline - you see exactly which endpoint served it.' },
        { tag: 'store', title: 'Bring your own store', body: 'A GatewayStore trait handles persistence. Wire in whatever you already run - gateway ships no DB.' }
      ]
    },

    crates: {
      eyebrow: 'Two crates',
      title: 'Cloud and local, one config.',
      items: [
        {
          name: 'gateway',
          version: VERSION,
          body: 'Provider-agnostic routing engine. Trait-based adapters, named fallback chains, per-endpoint circuit breaker, budget filtering and request tracing - with a store trait for persistence.',
          deps: ['reqwest', 'rustls', 'tokio'],
          note: ''
        },
        {
          name: 'gateway-embedded',
          version: VERSION,
          body: 'In-process inference adapters and an on-disk model registry. The same InferenceAdapter trait as the cloud adapters, so local and cloud models compose in one routing config.',
          deps: ['llama-cpp', 'fastembed', 'ort'],
          note: 'feature-gated'
        }
      ]
    },

    usage: {
      eyebrow: 'Consuming it',
      title: 'Pin a tag. Lock the commit.',
      body: 'Add it as a git dependency on a tagged release. Cargo.lock in your binary pins the exact commit, so there is no silent drift between builds. Developing in-place? Clone next to your consumer and add a dev-only [patch] at the workspace root.',
      footnote: '// after editing locally: push, cut a new tag, bump the pinned tag in each consumer',
      tabs: [
        {
          id: 'add', label: 'Cargo.toml', code: [
            '[dependencies]',
            'gateway          = { git = "' + REPO + '", tag = "' + VERSION + '" }',
            'gateway-embedded = { git = "' + REPO + '", tag = "' + VERSION + '", features = ["fastembed"] }'
          ].join('\n')
        },
        {
          id: 'patch', label: 'Local dev', code: [
            '# consumer workspace root - keep dev-only',
            '[patch."' + REPO + '"]',
            'gateway          = { path = "../gateway/crates/gateway" }',
            'gateway-embedded = { path = "../gateway/crates/gateway-embedded" }'
          ].join('\n')
        },
        {
          id: 'features', label: 'Embedded engines', code: [
            '# gateway-embedded features (all off by default)',
            'llama-cpp   # GGUF generation/embedding via llama.cpp',
            'fastembed   # lightweight embeddings',
            'ort         # ONNX Runtime (CPU)'
          ].join('\n')
        }
      ]
    },

    architecture: {
      eyebrow: 'Architecture',
      title: 'One trait, many backends.',
      caption: 'one InferenceAdapter trait - cloud and local backends compose in a single routing config',
      app: { title: 'Your app', sub: 'route(model)' },
      engine: { title: 'gateway', sub: 'routing engine', caps: ['fallback', 'circuit breaker', 'budget', 'tracing'], notes: ['no DB - GatewayStore trait', 'reqwest - rustls - tokio'] },
      adapter: { line1: 'Inference', line2: 'Adapter', sub: 'one trait' },
      cloud: { title: 'Cloud providers', sub: '~15 adapters', pills: ['openai', 'anthropic', 'google', 'mistral', 'groq'] },
      local: { title: 'Local engines', sub: 'in-process', pills: ['llama.cpp', 'onnx', 'fastembed'] }
    },

    consumers: {
      eyebrow: 'Shipped in production',
      title: 'Consumed by sibling projects.',
      body: 'gateway is the shared inference layer under two projects in the sensei-hq org.',
      items: [
        { mark: 's', name: 'sensei', repo: 'github.com/sensei-hq/sensei', href: 'https://github.com/sensei-hq/sensei' },
        { mark: 'o', name: 'strategos', repo: 'github.com/sensei-hq/strategos', href: 'https://github.com/sensei-hq/strategos' }
      ]
    },

    versioning: {
      eyebrow: 'Versioning',
      title: 'Independent, semver, reproducible.',
      body: 'This repo versions independently of its consumers. Releases are semver tags - both crates currently share ' + VERSION + ', carried over from sensei.',
      steps: [
        { n: '01', title: 'Tag with semver', note: 'vMAJOR.MINOR.PATCH' },
        { n: '02', title: 'Consumer pins the tag', note: 'git dependency, tag = "' + VERSION + '"' },
        { n: '03', title: 'Cargo.lock pins the commit', note: 'exact commit - no silent drift' }
      ]
    },

    cta: {
      title: 'Start routing through gateway.',
      body: 'Add the git dependency, name a fallback chain, and let the engine handle the rest.',
      primary: { label: 'Get started guide', href: REPO },
      secondary: { label: 'Star on GitHub', href: REPO }
    },

    footer: {
      blurb: 'Shared LLM inference routing engine for Rust. Fallback chains, circuit breaker, budget management - cloud and local.',
      columns: [
        { title: 'Crates', links: [{ label: 'gateway', href: REPO }, { label: 'gateway-embedded', href: REPO }] },
        { title: 'Resources', links: [{ label: 'GitHub', href: REPO }, { label: 'Releases', href: REPO + '/releases' }, { label: 'MIT License', href: REPO }] }
      ],
      legal: 'MIT licensed - sensei-hq'
    }
  };

  if (typeof window !== 'undefined') window.__GATEWAY_CONTENT = content;
  if (typeof module !== 'undefined' && module.exports) module.exports = content;
})();
