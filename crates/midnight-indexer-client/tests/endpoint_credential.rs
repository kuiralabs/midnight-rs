//! What actually reaches the server for an explicit endpoint: the exact path
//! (nothing appended) and the credential as a header, on both transports.
//!
//! Asserted from the server's side of a local socket, because a URL-shape
//! assertion passes against a client that never sends the header.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use midnight_indexer_client::testutil::bind;
use midnight_indexer_client::{
    IndexerClient, IndexerCredential, IndexerEndpoint, SubscriptionClient,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};

const SECRET: &str = "nightpreprodTESTVALUE";

/// The upgrade request's path and its `project_id` header, as the server saw them.
type UpgradeSeen = (String, Option<String>);

fn blockfrost_shaped(base: &str, credential: Option<IndexerCredential>) -> IndexerEndpoint {
    IndexerEndpoint::new(
        format!("{base}/api/v0"),
        format!("{base}/api/v0/ws"),
        credential,
    )
}

/// Serve one HTTP request and return its head (request line + headers).
async fn capture_one_http_request(listener: TcpListener) -> String {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "client closed before sending a request");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let body = r#"{"data":{"block":null}}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
    head
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().skip(1).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

#[tokio::test]
async fn http_posts_to_the_given_url_with_the_credential_as_a_header() {
    let (listener, base) = bind().await;
    let server = tokio::spawn(capture_one_http_request(listener));

    let endpoint = blockfrost_shaped(&base, Some(IndexerCredential::new("project_id", SECRET)));
    let client = IndexerClient::new(endpoint).unwrap();
    client.get_block(None).await.expect("query");

    let head = server.await.unwrap();
    let request_line = head.lines().next().unwrap();
    assert_eq!(request_line, "POST /api/v0 HTTP/1.1", "{head}");
    assert_eq!(header(&head, "project_id"), Some(SECRET), "{head}");
}

#[tokio::test]
async fn the_self_hosted_layout_sends_no_credential() {
    let (listener, base) = bind().await;
    let server = tokio::spawn(capture_one_http_request(listener));

    IndexerClient::new(base.as_str())
        .unwrap()
        .get_block(None)
        .await
        .expect("query");

    let head = server.await.unwrap();
    assert_eq!(
        head.lines().next().unwrap(),
        "POST /api/v4/graphql HTTP/1.1",
        "{head}"
    );
    assert_eq!(header(&head, "project_id"), None, "{head}");
}

#[tokio::test]
async fn the_subscription_connects_to_the_given_url_with_the_credential_as_a_header() {
    let (listener, base) = bind().await;
    let seen: Arc<Mutex<Option<UpgradeSeen>>> = Arc::default();
    let seen_by_server = seen.clone();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        #[allow(clippy::result_large_err)]
        let callback =
            move |req: &Request, mut resp: Response| -> Result<Response, ErrorResponse> {
                let credential = req
                    .headers()
                    .get("project_id")
                    .map(|v| v.to_str().unwrap().to_string());
                *seen_by_server.lock().unwrap() = Some((req.uri().to_string(), credential));
                if let Some(proto) = req.headers().get("Sec-WebSocket-Protocol") {
                    resp.headers_mut()
                        .insert("Sec-WebSocket-Protocol", proto.clone());
                }
                Ok(resp)
            };
        // Holding the socket open is enough: the handshake is what is under test.
        let _ws = tokio_tungstenite::accept_hdr_async(stream, callback)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let endpoint = blockfrost_shaped(&base, Some(IndexerCredential::new("project_id", SECRET)));
    let client = SubscriptionClient::new(endpoint).with_connect_timeout(Duration::from_millis(500));
    // The mock never acks, so the subscribe itself times out; the upgrade request has been seen.
    let _ = client
        .subscribe::<serde_json::Value>("subscription { x }", serde_json::json!({}))
        .await;

    let (path, credential) = seen
        .lock()
        .unwrap()
        .clone()
        .expect("the server saw an upgrade request");
    assert_eq!(path, "/api/v0/ws");
    assert_eq!(credential.as_deref(), Some(SECRET));
    server.abort();
}
