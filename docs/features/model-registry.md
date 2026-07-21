# Model Registry

Resolves a stable model **id** to an on-disk path to the model bytes, drawing
from three storage sources looked up in a fixed precedence order. Each source is
a separate resolver; they compose into one registry via `ChainedResolver`.

Source: `crates/local-engine/src/registry/mod.rs`,
`crates/local-engine/src/registry/managed.rs`,
`crates/local-engine/src/registry/ollama.rs`,
`crates/local-engine/src/registry/external.rs`;
the `ModelEntry`/`ModelResolver` vocabulary lives in `crates/kernel/src/registry.rs`.

## Purpose

Adapters and workflows refer to models by a stable `id` (e.g.
`"all-minilm-l6-v2-f16"`) that is intended to stay constant across sensei
versions. The registry maps that id to a concrete file the caller can `mmap` or
`open`, regardless of whether the bytes are owned by sensei, borrowed
read-through from an Ollama cache, or a loose file the user pointed at. The
three sources are looked up in order and the first hit wins.

## The `ModelResolver` trait

```rust
#[async_trait::async_trait]
pub trait ModelResolver: Send + Sync {
    async fn resolve(&self, id: &str) -> Result<Option<ModelEntry>, ResolveError>;
    async fn list(&self) -> Result<Vec<ModelEntry>, ResolveError>;
}
```

A resolver looks up models by stable id from **one** storage backend.

- `resolve(id)` returns `Ok(Some(entry))` on a hit, `Ok(None)` when this
  resolver simply does not know about that id, and `Err(..)` **only** for genuine
  backend failures — a broken/malformed manifest or an I/O error. The
  "not found" case is deliberately *not* an error; it is `Ok(None)` so that a
  chain can fall through to the next resolver. This contract is repeated in the
  doc comments of every implementation (e.g. `OllamaResolver::read_manifest_entry`
  returns `Ok(None)` for a manifest with no model layer or a missing blob, and
  `Err` only for unreadable files / invalid JSON).
- `list()` enumerates every model the resolver currently knows about.

Errors are the `ResolveError` enum: `Io(std::io::Error)`,
`InvalidManifest { path, message }` (a manifest or index file that was
unreadable or malformed), and `Serde(serde_json::Error)`.

## `ChainedResolver` precedence

`ChainedResolver` holds an ordered `Vec<Arc<dyn ModelResolver>>` and is itself a
`ModelResolver`. Resolvers are appended with the builder-style `push`, and the
chain dispatches in insertion order.

- **`resolve`**: iterates the resolvers in order and returns the first
  `Ok(Some(entry))`. Any `Err` short-circuits and propagates. If every resolver
  returns `Ok(None)`, the chain returns `Ok(None)`.
- **`list`**: concatenates every resolver's `list()` output, then de-duplicates
  by `id` keeping the **earlier** entry. De-dup uses a `HashSet` of ids with
  `all.retain(|e| seen.insert(e.id.clone()))`, so the first occurrence of an id
  survives and later resolvers are shadowed.

The intended registry precedence is **Managed → Ollama → External**, documented
in the module header and in the `ChainedResolver` doc comment. Note that the
precedence is not hard-coded into `ChainedResolver` itself — it is purely a
function of the order the caller `push`es resolvers. The type is order-agnostic;
"Managed wins over Ollama wins over External" is a convention enforced at
construction time, not by this struct.

```rust
let registry = ChainedResolver::new()
    .push(Arc::new(managed))    // Managed source — highest precedence
    .push(Arc::new(ollama))     // Ollama read-through
    .push(Arc::new(external));  // user-pointed paths — lowest precedence
```

## `ModelFormat` and `ModelEntry`

`ModelFormat` is the on-disk encoding of a model file. It serializes
`snake_case` (`"gguf"`, `"onnx"`, `"safetensors"`):

| Variant | Meaning |
| --- | --- |
| `Gguf` | GGUF container (llama.cpp ecosystem) |
| `Onnx` | ONNX graph |
| `Safetensors` | HuggingFace safetensors |

`ModelEntry` is a registered model:

| Field | Type | Notes |
| --- | --- | --- |
| `id` | `String` | Stable sensei id, e.g. `"all-minilm-l6-v2-f16"`. What adapters/workflows refer to. |
| `name` | `String` | Display name for UI. |
| `format` | `ModelFormat` | On-disk format. |
| `source` | `ModelSource` | Where the bytes live and who owns them. |
| `sha256` | `Option<String>` | Content hash for integrity verification on load. `skip_serializing_if = "Option::is_none"`. |
| `size_bytes` | `Option<u64>` | File size if known at registration. `skip_serializing_if = "Option::is_none"`. |

`ModelEntry` round-trips through JSON preserving the source kind (verified by
test). `ModelSource::path()` returns the on-disk bytes path for any variant,
so callers never need to match on the kind just to open the file.

## The three sources

`ModelSource` is a `#[serde(tag = "kind", rename_all = "snake_case")]` enum, so
each variant serializes with a `"kind"` discriminator (`"managed"`, `"ollama"`,
`"external"`).

### `ModelSource::Managed` — sensei-owned (read-write)

```rust
Managed { path: PathBuf }
```

Files sensei owns under a managed root (documented as `~/.sensei/models/`).
Sensei may delete, replace, or GC unreferenced files. Backed by
`ManagedResolver`, the only **read-write** resolver.

State lives in an `index.json` at the root, shaped
`{ "version": 1, "models": [ModelEntry, ...] }` (the file constant is
`INDEX_FILE = "index.json"`, `CURRENT_VERSION = 1`). Key behaviours:

- **No in-memory cache.** Every `resolve`/`list` calls `load()`, which reads
  `index.json` fresh. A missing file is not an error: `NotFound` maps to
  `Index::default()` (version 1, empty models), so reads against a
  non-existent root return an empty list. Malformed JSON surfaces as
  `ResolveError::InvalidManifest { path, .. }`.
- **`resolve`** returns the first model whose `id` matches; **`list`** returns
  all models from the index.
- **Writes are serialized and atomic.** `add`/`remove` take a single-process
  `Mutex<()>` write lock, then write `index.json.tmp`
  (`INDEX_TMP`) and `rename` it over `index.json`
  (`save_atomic`) so a partial write cannot corrupt the index.
- **`add`** replaces any prior entry with the same id (`retain(|m| m.id != …)`
  then `push`). The caller must place the model file under the managed root
  first; the resolver only records metadata.
- **`remove`** returns `true` if the id existed; it does **not** delete the
  underlying file — only the index entry. It only rewrites the index if
  something was actually removed.
- `add` uses `debug_assert!` to require a `Managed` source. See the note on
  release builds below.

Helpers: `index_path()` (`<root>/index.json`) and `root()`.

### `ModelSource::Ollama` — read-through of a local Ollama cache (never written)

```rust
Ollama {
    manifest: PathBuf,     // the manifest file walked to discover the blob
    blob_digest: String,   // digest with the "sha256:" prefix stripped
    blob_path: PathBuf,    // <root>/blobs/sha256-<digest>
}
```

A read-through view of a model already pulled into a local Ollama cache
(typically `~/.ollama/models`). The Ollama daemon owns the bytes and may GC
unreferenced blobs; sensei **never writes** into this store. `manifest` is kept
for diagnostics and re-resolution. `ModelSource::path()` returns `blob_path`.

`OllamaResolver` operates by read-only recursive walks:

- `walk_files` recursively collects every regular file under
  `<root>/manifests`. A non-existent tree yields an empty vec (each `read_dir`
  `NotFound` is skipped), so an absent/unreadable cache resolves as "no models"
  rather than an error.
- Each manifest is JSON with a `layers` array. `read_manifest_entry` parses it
  (`OllamaManifest`/`OllamaLayer`), then finds the layer whose `mediaType` is
  `application/vnd.ollama.image.model` (constant `MODEL_MEDIA_TYPE`). If no such
  layer exists (e.g. a license/template-only manifest), it returns `Ok(None)` —
  skipped silently, not an error.
- The layer `digest` has any `sha256:` prefix trimmed and becomes `blob_digest`.
  The blob is expected at `<root>/blobs/sha256-<digest>`. If that file is
  **missing** (post-GC), the manifest is `warn!`-logged and skipped as
  `Ok(None)` — the model becomes invisible, not an error.
- On success it emits a `ModelEntry` with `format: ModelFormat::Gguf`
  (GGUF is assumed for every Ollama model layer — see surprises), `name == id`,
  `sha256 = Some(<digest>)`, and `size_bytes` from the layer's optional `size`.
- Corrupt/invalid JSON is the only thing that surfaces as
  `ResolveError::InvalidManifest`.

`resolve(id)` walks all manifests and short-circuits on the first entry whose id
equals `canonical_id(id)`; `list()` walks and collects them all. There is no
caching — every call re-walks the tree (the code notes a fresh cache has only
"~10s of manifests").

### `ModelSource::External` — arbitrary user paths (linked in place)

```rust
External { path: PathBuf }
```

An arbitrary path supplied by the user (e.g. a hand-downloaded GGUF). The file
stays where the user put it — it is **linked in place**, never moved. Taking
ownership ("Move to library") is a higher-level operation that promotes the
entry to a `Managed` source via `ManagedResolver`; it is *not* done by this
resolver.

`ExternalResolver` is a purely in-memory `RwLock<HashMap<String, ModelEntry>>`
with **no filesystem index** — entries are registered at runtime (e.g. from app
settings):

- `register(entry)` inserts by id, replacing any prior entry with the same id.
- `unregister(id)` removes and returns the entry, or `None`.
- `resolve`/`list` read from the map (cloning entries out).
- `register` uses `debug_assert!` to require an `External` source. See below.

## How Ollama manifest paths map to ids

`id_from_manifest_path` turns a manifest path into a sensei id from the layout
`<root>/manifests/<registry>/<namespace>/<name>/<tag>`. It strips the manifests
root prefix and requires **exactly four** path components — any other shape
returns `None` (the entry is skipped rather than guessed at).

The defaults that Ollama treats as implicit are stripped:

- `DEFAULT_REGISTRY = "registry.ollama.ai"`
- `DEFAULT_NAMESPACE = "library"`
- `DEFAULT_TAG = "latest"`

The base id is built as:

| Condition | Base id |
| --- | --- |
| registry == default **and** namespace == default | `name` |
| registry == default (non-default namespace) | `namespace/name` |
| non-default registry | `registry/namespace/name` |

Then the tag is appended unless it is `latest`: `latest` is dropped, any other
tag becomes `base:tag`.

Examples (from the tests):

| Manifest path (under `manifests/`) | Id |
| --- | --- |
| `registry.ollama.ai/library/all-minilm/latest` | `all-minilm` |
| `registry.ollama.ai/library/qwen/7b` | `qwen:7b` |
| `registry.ollama.ai/myorg/private/latest` | `myorg/private` |
| `registry.ollama.ai/myorg/bar/v2` | `myorg/bar:v2` |

On the query side, `canonical_id(id)` normalizes an incoming id by stripping a
trailing `:latest` so callers can resolve `all-minilm` and `all-minilm:latest`
interchangeably.

## Notes, discrepancies, and surprises

- **`debug_assert!` source-kind checks are release no-ops.** Both
  `ManagedResolver::add` and `ExternalResolver::register` guard their source
  kind with `debug_assert!`. `ExternalResolver`'s doc comment says it "panics in
  debug builds if a foreign source kind is added" — but in a release build the
  assertion is compiled out, so a mismatched source kind would be accepted
  silently. The invariant is only enforced in debug/test builds.
- **Precedence is a construction convention, not a type guarantee.**
  `ChainedResolver` dispatches strictly in `push` order; the documented
  Managed → Ollama → External ordering depends entirely on the caller pushing in
  that order. Nothing in the type prevents a different order.
- **Ollama `sha256` duplicates `blob_digest`.** For an Ollama entry, both the
  `ModelEntry.sha256` field and `ModelSource::Ollama.blob_digest` are set to the
  same prefix-stripped digest (`digest.trim_start_matches("sha256:")`). The
  digest is effectively computed/stored twice.
- **GGUF is hard-assumed for Ollama models.** `OllamaResolver` always emits
  `ModelFormat::Gguf`; it never sniffs the blob. The code notes that if Ollama
  ever ships ONNX/safetensors, the format will need to be derived from the
  blob's magic bytes.
- **Query canonicalization only handles `:latest`, not namespace/registry.**
  Id *construction* strips the default registry and `library` namespace, but
  `canonical_id` on the query side only strips `:latest`. So a stored id of
  `all-minilm` is reachable as `all-minilm` or `all-minilm:latest`, but not as
  the fully-qualified `registry.ollama.ai/library/all-minilm`.
- **No caching anywhere.** `ManagedResolver` re-reads `index.json` on every
  call and `OllamaResolver` re-walks the manifest tree on every call. This is a
  deliberate simplicity trade-off documented in the source (tiny index; small
  manifest count).
- **`ManagedResolver::remove` and the file are decoupled.** Removing an index
  entry never touches the model bytes on disk; GC of the actual file is left to
  a higher layer.
