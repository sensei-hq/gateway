//! The gateway's in-memory stores keep the conformance contract (TM-3). The Postgres stores run
//! the same suite in `orchestrator-store`'s Postgres tests; torii's stores run it after the move.

use orchestrator_store::{
    InMemoryConfigStore, InMemoryContentStore, InMemoryContextStore, InMemoryJournal,
    InMemorySchedulerStore,
};

#[tokio::test]
async fn in_memory_journal() {
    orchestrator_testkit::journal(&InMemoryJournal::default()).await;
}

#[tokio::test]
async fn in_memory_content() {
    orchestrator_testkit::content(&InMemoryContentStore::default()).await;
}

#[tokio::test]
async fn in_memory_context() {
    let cas = std::sync::Arc::new(InMemoryContentStore::default());
    orchestrator_testkit::context(&InMemoryContextStore::new(cas)).await;
}

#[tokio::test]
async fn in_memory_scheduler() {
    orchestrator_testkit::scheduler(&InMemorySchedulerStore::new()).await;
}

#[tokio::test]
async fn in_memory_config_store() {
    orchestrator_testkit::config_store(&InMemoryConfigStore::new()).await;
}
