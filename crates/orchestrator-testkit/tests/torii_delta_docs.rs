//! AG-3 (#87) review: the docs that tell torii what its `PgSchedulerStore` must change to pass
//! `orchestrator_testkit::scheduler` must name the WHOLE delta. Overriding only
//! `begin_wake_attempt`/`record_wake_failed` still fails the testkit: `claim_due`'s stale-`waking`
//! arm must also honour the armed retry deadline, and `enqueue`/`record_paused` must set/reset the
//! attempt count. A doc that understates this sends the torii implementer into a red testkit.

use std::path::PathBuf;

fn repo_file(rel: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// Every element of the torii delta the in-memory store needed to pass the extended testkit.
const DELTA: &[&str] = &[
    "begin_wake_attempt",
    "record_wake_failed",
    "claim_due",
    "next_wake IS NULL OR next_wake <= $now",
    "attempts",
    "last_wake_error",
    "enqueue",
    "record_paused",
];

fn assert_names_full_delta(doc: &str, passage: &str) {
    let missing: Vec<&&str> = DELTA.iter().filter(|t| !passage.contains(**t)).collect();
    assert!(
        missing.is_empty(),
        "{doc}: the AG-3 torii delta omits {missing:?}:\n{passage}"
    );
}

#[test]
fn the_orchestrator_readme_names_the_full_torii_scheduler_delta() {
    let readme = repo_file("docs/features/orchestrator/README.md");
    // The "Implementing a store?" callout, up to the blank line that ends it.
    let start = readme
        .find("**Implementing a store?**")
        .expect("README keeps its 'Implementing a store?' callout");
    let passage = readme[start..].split("\n\n").next().unwrap();
    assert_names_full_delta("README.md", passage);
}

#[test]
fn the_overview_carry_forwards_name_the_full_torii_scheduler_delta() {
    let overview = repo_file("docs/superpowers/orchestrator-overview.md");
    assert!(
        !overview.contains("overrides the two new trait methods"),
        "the overview still says torii needs only the two new trait methods"
    );
    let marker = "live in torii once its `PgSchedulerStore`";
    let hits: Vec<_> = overview.match_indices(marker).collect();
    assert_eq!(hits.len(), 2, "both AG-3 carry-forward lines are present");
    for (at, _) in hits {
        let end = overview[at..].find('\n').map_or(overview.len(), |n| at + n);
        let passage = &overview[at..end.min(at + 1200)];
        assert_names_full_delta("orchestrator-overview.md", passage);
    }
}

/// The trait doc is the authoritative surface for `begin_wake_attempt`: it must not claim torii's
/// `PgSchedulerStore` already overrides it. Until it does, production keeps the pre-AG-3 crash
/// loop, and a reader of the trait must be told so.
#[test]
fn the_begin_wake_attempt_doc_does_not_claim_torii_already_overrides_it() {
    let src = repo_file("crates/orchestrator-core/src/scheduler.rs");
    let end = src
        .find("async fn begin_wake_attempt(")
        .expect("the trait declares begin_wake_attempt");
    let start = src[..end]
        .rfind("/// AG-3: a claimed wake is about to be driven")
        .expect("begin_wake_attempt keeps its AG-3 doc");
    let doc = &src[start..end];
    assert!(
        !doc.contains("Both shipped stores override it"),
        "the doc claims both shipped stores override begin_wake_attempt; only the in-memory \
         store does:\n{doc}"
    );
    assert!(
        doc.contains("PgSchedulerStore") && doc.contains("crash loop"),
        "the doc must say torii's PgSchedulerStore does not override it yet, and what that \
         costs:\n{doc}"
    );
}
