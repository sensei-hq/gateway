## gateway — LLM inference routing library
##
## Cargo workspace:
##   crates/kernel           — shared types, capability traits, adapter registry (sensei-kernel)
##   crates/cloud-providers  — cloud provider adapters (sensei-cloud-providers)
##   crates/gateway          — routing engine (fallback chains, circuit breaker, budgets)
##   crates/local-providers  — in-process inference adapters, opt-in native deps (sensei-local-providers)
##   crates/local-engine     — model resolvers + Hugging Face pull (sensei-local-engine)
##   crates/vault            — shared BYOK credential vault (envelope crypto, KEK/DEK) (sensei-vault)
##
## `make bump` auto-includes every crate via the crates/*/Cargo.toml glob, so a new
## crate versions in lockstep with no Makefile change — this list is descriptive only.
##
## Consumed by sensei (sensei-hq/sensei) as a git dependency (`gateway` /
## `local-providers` / `local-engine`) pinned by tag. A release here is just a
## tag: `make bump` bumps every crate version + the site package in lockstep,
## commits, tags, and pushes — then
## sensei re-pins the git dep to the new tag. There are no binaries to publish
## (this is a library), so the tag push has no release artifacts to build.
##
## Versioning:
##   Every crate + the site package (site/package.json) share one version (kept
##   in lockstep). The current version is read from crates/gateway/Cargo.toml —
##   that is the single source of truth.

.PHONY: help build test test-fast fmt fmt-check clippy lint cov cov-check cov-html \
        check bump release clean sweep hooks

# Single source of truth: the [package] version of the gateway crate.
VERSION := $(shell grep -m1 '^version = ' crates/gateway/Cargo.toml | sed -E 's/version = "(.*)"/\1/')

# ── Help ──────────────────────────────────────────────────────────────────────

help: ## Show this help message
	@grep -E '^[a-zA-Z0-9_-]+:.*## .*$$' $(MAKEFILE_LIST) \
	  | sort \
	  | awk 'BEGIN {FS = ":.*## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  current version: $(VERSION)"

# ── Build / test ──────────────────────────────────────────────────────────────

build: ## Build the whole workspace (default features)
	cargo build --workspace

test: ## Run the full test suite (workspace, default features)
	cargo test --workspace

test-fast: ## Run only the gateway crate's lib unit tests (no wiremock integration)
	cargo test -p sensei-gateway --lib

# ── Lint / format ─────────────────────────────────────────────────────────────

fmt: ## Format all code
	cargo fmt --all

fmt-check: ## Check formatting without modifying files
	cargo fmt --all --check

clippy: ## Lint with clippy, warnings-as-errors
	cargo clippy --workspace --all-targets -- -D warnings

lint: fmt-check clippy ## fmt-check + clippy

# ── Git hooks ───────────────────────────────────────────────────────────────
# The tracked hook in .githooks/ runs `make lint` (fmt-check + clippy) before
# every commit, so formatting/lint never drifts in. Opt in per clone with
# `make hooks` (sets core.hooksPath); bypass a single commit with
# `git commit --no-verify`.

hooks: ## Install the tracked git pre-commit hook (fmt-check + clippy)
	git config core.hooksPath .githooks
	@echo "Installed: git pre-commit runs 'make lint' (core.hooksPath=.githooks)."

# ── Coverage ──────────────────────────────────────────────────────────────────
# Requires cargo-llvm-cov: cargo install cargo-llvm-cov
# local-providers' native adapters (llama-cpp/ort/fastembed) are behind feature
# flags and need a C/C++ toolchain, so coverage targets the gateway crate — the
# routing engine and provider adapters that carry the testable logic.

# `cov` / `cov-check` reclaim their instrumented build afterwards (a separate, rarely-reused
# target tree) while keeping the command's own exit status. `cov-html` does NOT: it would delete
# the report it just opened — run `make clean` when done reading it.
cov: ## Print a per-file coverage summary for the gateway crate (then reclaims the coverage build)
	@ok=0; cargo llvm-cov -p sensei-gateway --summary-only || ok=$$?; \
	 cargo llvm-cov clean >/dev/null 2>&1 || true; exit $$ok

cov-check: ## Fail if gateway line coverage drops below 80% (the CI gate; reclaims afterwards)
	@ok=0; cargo llvm-cov -p sensei-gateway --summary-only --fail-under-lines 80 || ok=$$?; \
	 cargo llvm-cov clean >/dev/null 2>&1 || true; exit $$ok

cov-html: ## Generate + open an HTML coverage report for the gateway crate
	cargo llvm-cov -p sensei-gateway --html --open

# ── Release gate ──────────────────────────────────────────────────────────────
# The tree is rustfmt-formatted and clippy-clean (as of the capability-trait
# refactor), so the gate now runs fmt-check + clippy alongside build + test.
# NOTE: clippy/build here use default features; the feature-gated embedded
# adapters (llama-cpp/ort/fastembed) need a C/C++ toolchain and are verified
# separately (see the cov note).

check: fmt-check clippy build test ## Pre-release gate: fmt + clippy + build + test

# ── Version bump / release ────────────────────────────────────────────────────
# Usage:
#   make bump v=patch    — 0.2.24 → 0.2.25
#   make bump v=minor    — 0.2.24 → 0.3.0
#   make bump v=major    — 0.2.24 → 1.0.0
#   make bump v=0.5.0    — explicit version
#
# Bumps every crate + the site version in lockstep, commits, tags vX.Y.Z, and pushes the
# commit + tag. Runs `make check` first so a broken build never gets tagged, and
# reclaims the local build cache (`cargo clean`) afterwards.
# Safety: aborts on a pre-existing tag, a downgrade, or a no-op (same version).

release: bump ## Alias for `bump` (a release here is just a tag push)

bump: ## Bump version, commit, tag, push (v=patch|minor|major|<version>)
	@if [ -z "$(v)" ]; then \
	  echo "Usage: make bump v=patch|minor|major|<version>  (current: $(VERSION))"; \
	  exit 1; \
	fi
	$(eval _v := $(shell \
	  cur="$(VERSION)"; \
	  if [ "$(v)" = "patch" ]; then echo "$$cur" | awk -F. '{printf "%s.%s.%s", $$1, $$2, $$3+1}'; \
	  elif [ "$(v)" = "minor" ]; then echo "$$cur" | awk -F. '{printf "%s.%s.0", $$1, $$2+1}'; \
	  elif [ "$(v)" = "major" ]; then echo "$$cur" | awk -F. '{printf "%s.0.0", $$1+1}'; \
	  else echo "$(v)"; \
	  fi))
	@# Safety: block if the target tag already exists.
	@if git tag -l "v$(_v)" | grep -q .; then \
	  echo "Error: tag v$(_v) already exists (current version is $(VERSION))."; \
	  echo "Did you mean: make bump v=patch ?"; \
	  exit 1; \
	fi
	@# Safety: block downgrades and no-op bumps.
	@cur="$(VERSION)"; \
	if [ "$$cur" = "$(_v)" ]; then \
	  echo "Error: $(_v) is already the current version"; exit 1; \
	fi; \
	if [ "$$(printf '%s\n%s' "$$cur" "$(_v)" | sort -V | tail -1)" = "$$cur" ]; then \
	  echo "Error: cannot bump down ($$cur → $(_v))"; exit 1; \
	fi
	@# Verify the tree is releasable BEFORE touching versions or git.
	@echo "Running pre-release gate (fmt + clippy + tests)..."
	@$(MAKE) check
	@echo "Bumping $(VERSION) → $(_v)"
	@# Every crate + the site package share one version — bump them in lockstep.
	@# The crates/*/Cargo.toml glob auto-includes any new crate; the anchored
	@# `^version = ` pattern matches only the [package] version line, never inline
	@# dep versions.
	@for f in crates/*/Cargo.toml; do \
	  sed -i '' -E "s/^version = \"[^\"]*\"/version = \"$(_v)\"/" "$$f"; \
	done
	@# The SvelteKit site (site/package.json) tracks the same version. Anchored to
	@# the indented top-level "version" key so nested dep versions are untouched.
	@sed -i '' -E "s/^([[:space:]]*\"version\"): \"[^\"]*\"/\1: \"$(_v)\"/" site/package.json
	@# Cargo.lock records every WORKSPACE MEMBER's version, so the sed above makes
	@# it stale. CI builds with `--locked`, which refuses to update it — v0.6.0
	@# shipped a tag whose lockfile said 0.5.1 while every manifest said 0.6.0, and
	@# both required checks failed in under 70s. Refresh it here rather than relying
	@# on the pre-commit hook's clippy run, which touches it only as a side effect
	@# and does so AFTER `git add`, leaving the change unstaged.
	@cargo metadata --format-version 1 --offline >/dev/null 2>&1 \
	  || cargo metadata --format-version 1 >/dev/null
	@git add crates/*/Cargo.toml site/package.json Cargo.lock
	@git commit -m "chore: bump to v$(_v)"
	@git tag -a "v$(_v)" -m "gateway v$(_v)"
	@git push origin HEAD
	@git push origin "v$(_v)"
	@# A release just built the whole workspace via `make check`, so reclaim the
	@# local build cache (target/ fills disk) now that the tag is pushed.
	@echo "Reclaiming local build cache (cargo clean)…"
	@$(MAKE) clean
	@echo "Pushed v$(_v). Re-pin the gateway / local-providers / local-engine git dep in sensei to tag v$(_v)."

# ── Clean / disk ──────────────────────────────────────────────────────────────
# target/ is what fills the disk (it reached 26 GB here after two releases cut by hand rather
# than through `make bump`, which reclaims). `bump` runs `clean` after the tag is pushed; a
# release done any other way must end with `make clean` too. `sweep` is the day-to-day tool: it
# prunes stale artifacts while keeping the current working set warm, where `clean` forces a
# full rebuild.

clean: ## Reclaim disk: remove target/ and report the MB actually freed
	@before=$$(du -sk target 2>/dev/null | awk '{print $$1}'); before=$${before:-0}; \
	 cargo clean; \
	 echo "target/ cleaned — $$(( before / 1024 )) MB reclaimed; the next build recompiles."

sweep: ## Prune STALE Rust artifacts (other toolchains, >14d untouched), keeping the build warm
	@if ! command -v cargo-sweep >/dev/null 2>&1; then \
	  echo "cargo-sweep not installed. Install it with:"; \
	  echo "  cargo install cargo-sweep"; \
	  echo "Or run 'make clean' to wipe target/ entirely (forces a full rebuild)."; \
	  exit 1; \
	fi
	@before=$$(du -sk target 2>/dev/null | awk '{print $$1}'); before=$${before:-0}; \
	 cargo sweep --installed; \
	 cargo sweep --time 14; \
	 after=$$(du -sk target 2>/dev/null | awk '{print $$1}'); after=$${after:-0}; \
	 echo "Swept — $$(( (before - after) / 1024 )) MB reclaimed, current working set kept warm."
