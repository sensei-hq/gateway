//! Integration tests for the Kling video-generation adapter using wiremock.
//!
//! Kling is an async job adapter: `generate_video()` first POSTs a submit
//! request to `{base}/videos/text2video` authenticated with a bearer token,
//! receiving `{"data":{"task_id":"..."}}`, then polls
//! `GET {base}/videos/text2video/{task_id}` until the returned
//! `data.task_status` is `"succeed"`. Because `JobConfig::default()` uses a
//! 3-second poll interval and `poll_until_complete` only sleeps when a poll
//! returns "still processing", every mocked poll endpoint here returns a
//! terminal status on its FIRST response so the tests stay sub-second.
//!
//! NOTE: unlike the FLUX adapter (which routes submit errors through
//! `http_json` and thus maps 401 -> Authentication, 403 -> ProviderError{status},
//! and 429 -> RateLimit), Kling's `generate_video()` maps EVERY non-success
//! submit status directly to
//! `GatewayError::ProviderError { status: Some(code), .. }`. These tests assert
//! that ACTUAL behavior.

use kernel::types::io::VideoRequest;

use cloud_providers::kling::KlingAdapter;
use kernel::adapters::capability::VideoModel;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[macro_use]
mod common;
use common::{assert_is_provider_error, router_config};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const SAMPLE_URL: &str = "https://cdn.klingai.com/output/generated-video.mp4";
const SUBMIT_PATH: &str = "/videos/text2video";

fn video_request() -> VideoRequest {
    VideoRequest {
        model: None,
        prompt: "a timelapse of a blooming flower".to_string(),
        duration_secs: Some(5),
        resolution: Some("1080p".to_string()),
    }
}

/// Mount the submit endpoint to return a task id with the given HTTP status.
async fn mount_submit_ok(server: &MockServer, task_id: &str) {
    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"data": {"task_id": task_id}})),
        )
        .mount(server)
        .await;
}

/// Submit succeeds, then the poll endpoint returns `poll`; assert the adapter maps it to a
/// ProviderError. The only axis that varies across the poll-failure cases is that poll
/// response — a terminal `"failed"` status, a 5xx, or an unparseable 200 body.
async fn assert_poll_error(task_id: &str, poll: ResponseTemplate) {
    let server = MockServer::start().await;
    mount_submit_ok(&server, task_id).await;
    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/{task_id}")))
        .respond_with(poll)
        .mount(&server)
        .await;

    let err = KlingAdapter::new()
        .unwrap()
        .generate_video(&router_config(&server.uri()), &video_request())
        .await
        .unwrap_err();
    assert_is_provider_error(&err);
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn kling_generate_video_happy_path() {
    let server = MockServer::start().await;

    // 1. Submit task -> returns a task_id.
    mount_submit_ok(&server, "task-abc-123").await;

    // 2. Poll -> returns a terminal "succeed" status with the video on the
    //    FIRST call.
    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-abc-123")))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "task_status": "succeed",
                "task_result": {
                    "videos": [
                        { "url": SAMPLE_URL, "duration": 5.0 }
                    ]
                }
            }
        })))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.generate_video(&config, &request).await.unwrap();

    let videos = response.videos;
    assert_eq!(videos.len(), 1);
    assert_eq!(videos[0].url.as_deref(), Some(SAMPLE_URL));
    assert!((videos[0].duration_secs.unwrap() - 5.0).abs() < f32::EPSILON);
}

/// The `succeed` branch with an empty result must still succeed, falling back
/// to the requested duration and a `None` URL.
#[tokio::test]
async fn kling_generate_video_succeed_without_video_falls_back() {
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-no-video").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-no-video")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "task_status": "succeed",
                "task_result": null
            }
        })))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let response = adapter.generate_video(&config, &request).await.unwrap();

    let videos = response.videos;
    assert_eq!(videos.len(), 1);
    assert!(videos[0].url.is_none());
    // Falls back to the requested duration (5s -> 5.0).
    assert!((videos[0].duration_secs.unwrap() - 5.0).abs() < f32::EPSILON);
}

// ---------------------------------------------------------------------------
// Submit error mappings
//
// Kling's generate_video() maps ALL non-success submit statuses directly to
// ProviderError { status: Some(code) }. There is no Authentication/RateLimit
// special-casing on this path.
// ---------------------------------------------------------------------------

http_error_tests! {
    call: |config| async move {
        KlingAdapter::new().unwrap().generate_video(&config, &video_request()).await
    },
    method: "POST",
    path: SUBMIT_PATH,
    cases: {
        kling_submit_401_maps_to_provider_error => (401, "invalid api key", common::ErrKind::Provider(Some(401))),
        kling_submit_403_maps_to_provider_error => (403, "forbidden", common::ErrKind::Provider(Some(403))),
        kling_submit_429_maps_to_provider_error => (429, "rate limited", common::ErrKind::Provider(Some(429))),
        kling_submit_500_maps_to_provider_error => (500, "internal server error", common::ErrKind::Provider(Some(500))),
    }
}

/// A 200 submit response whose body can't be parsed as the task envelope must
/// surface as a ProviderError.
#[tokio::test]
async fn kling_submit_unparseable_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(SUBMIT_PATH))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"unexpected": true})),
        )
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_is_provider_error(&err);
}

// ---------------------------------------------------------------------------
// Poll-phase failures
// ---------------------------------------------------------------------------

/// The poll returns a terminal `"failed"` status -> mapped ProviderError.
#[tokio::test]
async fn kling_poll_failed_status_maps_to_provider_error() {
    assert_poll_error(
        "task-fail-1",
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": { "task_status": "failed", "task_result": null }
        })),
    )
    .await;
}

/// A non-success HTTP status on the poll endpoint -> mapped ProviderError.
#[tokio::test]
async fn kling_poll_http_error_maps_to_provider_error() {
    assert_poll_error(
        "task-poll-500",
        ResponseTemplate::new(500).set_body_string("poll blew up"),
    )
    .await;
}

/// A 200 poll response whose body can't be parsed as the status envelope must
/// surface as a ProviderError.
#[tokio::test]
async fn kling_poll_unparseable_body_maps_to_provider_error() {
    assert_poll_error(
        "task-poll-garbage",
        ResponseTemplate::new(200).set_body_string("not json"),
    )
    .await;
}
