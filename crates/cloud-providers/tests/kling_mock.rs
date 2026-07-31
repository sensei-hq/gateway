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
//! `http_json` and thus maps 401/403 -> Authentication and 429 -> RateLimit),
//! Kling's `generate_video()` maps EVERY non-success submit status directly to
//! `GatewayError::ProviderError { status: Some(code), .. }`. These tests assert
//! that ACTUAL behavior.

use kernel::types::io::VideoRequest;

use cloud_providers::kling::KlingAdapter;
use kernel::adapters::capability::VideoModel;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod common;
use common::{assert_is_provider_error, assert_provider_error_status, mount_status, router_config};

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

#[tokio::test]
async fn kling_submit_401_maps_to_provider_error() {
    let server = MockServer::start().await;
    mount_status(&server, "POST", SUBMIT_PATH, 401, "invalid api key").await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_provider_error_status(&err, Some(401));
}

#[tokio::test]
async fn kling_submit_403_maps_to_provider_error() {
    let server = MockServer::start().await;
    mount_status(&server, "POST", SUBMIT_PATH, 403, "forbidden").await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_provider_error_status(&err, Some(403));
}

#[tokio::test]
async fn kling_submit_429_maps_to_provider_error() {
    let server = MockServer::start().await;
    mount_status(&server, "POST", SUBMIT_PATH, 429, "rate limited").await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_provider_error_status(&err, Some(429));
}

#[tokio::test]
async fn kling_submit_500_maps_to_provider_error() {
    let server = MockServer::start().await;
    mount_status(&server, "POST", SUBMIT_PATH, 500, "internal server error").await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_provider_error_status(&err, Some(500));
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
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-fail-1").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-fail-1")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": {
                "task_status": "failed",
                "task_result": null
            }
        })))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_is_provider_error(&err);
}

/// A non-success HTTP status on the poll endpoint -> mapped ProviderError.
#[tokio::test]
async fn kling_poll_http_error_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-poll-500").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-poll-500")))
        .respond_with(ResponseTemplate::new(500).set_body_string("poll blew up"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_is_provider_error(&err);
}

/// A 200 poll response whose body can't be parsed as the status envelope must
/// surface as a ProviderError.
#[tokio::test]
async fn kling_poll_unparseable_body_maps_to_provider_error() {
    let server = MockServer::start().await;

    mount_submit_ok(&server, "task-poll-garbage").await;

    Mock::given(method("GET"))
        .and(path(format!("{SUBMIT_PATH}/task-poll-garbage")))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let adapter = KlingAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = video_request();

    let err = adapter.generate_video(&config, &request).await.unwrap_err();
    assert_is_provider_error(&err);
}
