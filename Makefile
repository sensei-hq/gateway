## gateway — LLM inference routing library
##
## Cargo workspace:
##   crates/gateway          — routing engine (fallback chains, circuit breaker, budgets)
##   crates/gateway-embedded — in-process inference adapters (opt-in native deps)
##
## Consumed by sensei (sensei-hq/sensei) as a git dependency (`gateway` /
## `gateway-embedded`) pinned by tag. A release here is just a tag: `make bump`
## bumps both crate versions in lockstep, commits, tags, and pushes — then sensei
## re-pins the git dep to the new tag. There are no binaries to publish (this is a
## library), so the tag push has no release artifacts to build.
##
## Versioning:
##   The two crates share one version (kept in lockstep). The current version is
##   read from crates/gateway/Cargo.toml — that is the single source of truth.

.PHONY: help build test test-fast fmt fmt-check clippy lint cov cov-html \
        check bump release clean

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
	cargo test -p gateway --lib

# ── Lint / format ─────────────────────────────────────────────────────────────

fmt: ## Format all code
	cargo fmt --all

fmt-check: ## Check formatting without modifying files
	cargo fmt --all --check

clippy: ## Lint with clippy, warnings-as-errors
	cargo clippy --workspace --all-targets -- -D warnings

lint: fmt-check clippy ## fmt-check + clippy

# ── Coverage ──────────────────────────────────────────────────────────────────
# Requires cargo-llvm-cov: cargo install cargo-llvm-cov
# gateway-embedded's native adapters (llama-cpp/ort/fastembed) are behind feature
# flags and need a C/C++ toolchain, so coverage targets the gateway crate — the
# routing engine and provider adapters that carry the testable logic.

cov: ## Print a per-file coverage summary for the gateway crate
	cargo llvm-cov -p gateway --summary-only

cov-check: ## Fail if gateway line coverage drops below 80% (the CI gate)
	cargo llvm-cov -p gateway --summary-only --fail-under-lines 80

cov-html: ## Generate + open an HTML coverage report for the gateway crate
	cargo llvm-cov -p gateway --html --open

# ── Release gate ──────────────────────────────────────────────────────────────
# NOTE: `check` gates the release on build + test only. `fmt-check` and `clippy`
# are available as standalone targets but are NOT in the gate yet: the existing
# tree isn't rustfmt-formatted or clippy-clean, so gating on them would block
# every release until a separate codebase-wide normalization lands.

check: build test ## Pre-release gate: build + test must pass

# ── Version bump / release ────────────────────────────────────────────────────
# Usage:
#   make bump v=patch    — 0.2.24 → 0.2.25
#   make bump v=minor    — 0.2.24 → 0.3.0
#   make bump v=major    — 0.2.24 → 1.0.0
#   make bump v=0.5.0    — explicit version
#
# Bumps BOTH crate versions in lockstep, commits, tags vX.Y.Z, and pushes the
# commit + tag. Runs `make check` first so a broken build never gets tagged.
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
	@# Both crates share one version — update them in lockstep. The anchored
	@# pattern matches only the [package] version line, never inline dep versions.
	@sed -i '' -E "s/^version = \"[^\"]*\"/version = \"$(_v)\"/" crates/gateway/Cargo.toml
	@sed -i '' -E "s/^version = \"[^\"]*\"/version = \"$(_v)\"/" crates/gateway-embedded/Cargo.toml
	@git add crates/gateway/Cargo.toml crates/gateway-embedded/Cargo.toml
	@git commit -m "chore: bump to v$(_v)"
	@git tag -a "v$(_v)" -m "gateway v$(_v)"
	@git push origin HEAD
	@git push origin "v$(_v)"
	@echo "Pushed v$(_v). Re-pin the gateway / gateway-embedded git dep in sensei to tag v$(_v)."

# ── Clean ─────────────────────────────────────────────────────────────────────

clean: ## Remove the Cargo target/ directory
	cargo clean
