//! Mechanical contract for downloader behavior shared with Discogs ingestion.

use super::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

fn downloader_with_read_timeout(output_directory: PathBuf, base_url: String, read_timeout: Duration) -> MbDownloader {
    let client = PoliteClient::new(PoliteConfig {
        min_gap: Duration::ZERO,
        max_retry_after: Duration::from_millis(50),
        max_throttle_retries: 1,
        request_timeout: Duration::from_secs(1),
        read_timeout,
    })
    .unwrap();
    MbDownloader { output_directory, base_url, client }
}

fn entity_tar_xz(entity: &str, content: &[u8]) -> Vec<u8> {
    let mut tar_data = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_data);
        let mut header = tar::Header::new_gnu();
        header.set_path(format!("{entity}/mbdump/{entity}")).unwrap();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder.append(&header, content).unwrap();
        builder.finish().unwrap();
    }
    let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 1);
    encoder.write_all(&tar_data).unwrap();
    encoder.finish().unwrap()
}

fn read_jsonl_xz(path: &Path) -> Vec<u8> {
    let file = std::fs::File::open(path).unwrap();
    let mut decoder = xz2::read::XzDecoder::new(file);
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut bytes).unwrap();
    bytes
}

async fn read_request(socket: &mut TcpStream) -> String {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = socket.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&request).into_owned()
}

async fn write_response(socket: &mut TcpStream, body: &[u8]) {
    let headers = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    socket.write_all(headers.as_bytes()).await.unwrap();
    socket.write_all(body).await.unwrap();
    socket.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_attempt_is_cleaned_then_restarts_from_zero_after_backoff() {
    let stale_body = entity_tar_xz("artist", b"{\"id\":\"stale\"}\n");
    let fresh_content = b"{\"id\":\"fresh\"}\n";
    let fresh_body = entity_tar_xz("artist", fresh_content);
    let expected_hash = hex::encode(Sha256::digest(&fresh_body));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = requests.clone();
    let bodies = vec![stale_body, fresh_body];

    let server = tokio::spawn(async move {
        for body in bodies {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured_requests.lock().await.push(read_request(&mut socket).await);
            write_response(&mut socket, &body).await;
        }
    });

    let temp_dir = TempDir::new().unwrap();
    let out_path = temp_dir.path().join("artist.jsonl.xz");
    let downloader = downloader_with_read_timeout(temp_dir.path().to_path_buf(), format!("http://{address}/"), Duration::from_secs(1));
    let started = Instant::now();

    downloader
        .stream_download_verify_extract(&format!("http://{address}/artist.tar.xz"), "artist", &expected_hash, &out_path)
        .await
        .unwrap();
    server.await.unwrap();

    assert!(started.elapsed() >= Duration::from_millis(MB_RETRY_BASE_DELAY_MS));
    assert_eq!(read_jsonl_xz(&out_path), fresh_content);
    assert!(!tmp_download_path(&out_path).exists(), "failed attempt must not leave its staging file");
    let requests = requests.lock().await;
    assert_eq!(requests.len(), 2, "one bad checksum must consume exactly one retry");
    assert!(
        requests.iter().all(|request| !request.to_ascii_lowercase().contains("\r\nrange:")),
        "MusicBrainz retries restart the tarball rather than issuing byte-range resumes"
    );
}

#[tokio::test]
async fn stalled_body_times_out_three_attempts_and_leaves_no_published_or_partial_file() {
    let defaults = PoliteConfig::musicbrainz();
    assert_eq!(defaults.request_timeout, Duration::from_secs(120));
    assert_eq!(defaults.read_timeout, Duration::from_secs(120));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let request_count = Arc::new(Mutex::new(0_usize));
    let captured_count = request_count.clone();
    let server = tokio::spawn(async move {
        let mut stalled_connections = Vec::new();
        for _ in 0..MB_MAX_DOWNLOAD_RETRIES {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_request(&mut socket).await;
            *captured_count.lock().await += 1;
            stalled_connections.push(tokio::spawn(async move {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1024\r\nConnection: keep-alive\r\n\r\npartial").await.unwrap();
                socket.flush().await.unwrap();
                tokio::time::sleep(Duration::from_secs(10)).await;
            }));
        }
        stalled_connections
    });

    let temp_dir = TempDir::new().unwrap();
    let out_path = temp_dir.path().join("artist.jsonl.xz");
    let read_timeout = Duration::from_millis(50);
    let downloader = downloader_with_read_timeout(temp_dir.path().to_path_buf(), format!("http://{address}/"), read_timeout);
    let started = Instant::now();

    let error = downloader
        .stream_download_verify_extract(&format!("http://{address}/artist.tar.xz"), "artist", &"0".repeat(64), &out_path)
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    let stalled_connections = server.await.unwrap();
    for connection in stalled_connections {
        connection.abort();
    }

    assert!(elapsed >= read_timeout * MB_MAX_DOWNLOAD_RETRIES);
    assert!(elapsed < Duration::from_secs(2), "bounded body reads took {elapsed:?}");
    assert_eq!(*request_count.lock().await, MB_MAX_DOWNLOAD_RETRIES as usize);
    assert!(!out_path.exists(), "unverified bytes must never be published");
    assert!(!tmp_download_path(&out_path).exists(), "terminal failure must remove the staging file");
    let chain = format!("{error:#}").to_ascii_lowercase();
    assert!(chain.contains("timed out") || chain.contains("timeout"), "terminal error must retain the timeout cause: {chain}");
}

#[tokio::test]
async fn resume_skips_published_entity_but_restarts_leftover_tmp_entity() {
    let temp_dir = TempDir::new().unwrap();
    let version = "20260325-001001";
    let version_dir = temp_dir.path().join(version);
    std::fs::create_dir(&version_dir).unwrap();
    std::fs::write(version_dir.join("artist.jsonl.xz"), b"already verified").unwrap();
    std::fs::write(version_dir.join("label.jsonl.xz.tmp"), b"stale partial").unwrap();

    let mut server = mockito::Server::new_async().await;
    let base_url = format!("{}/", server.url());
    let index = format!(r#"<a href="{version}/">{version}/</a>"#);
    let _index = server.mock("GET", "/").with_status(200).with_body(index).create_async().await;
    let mut checksum_lines = String::new();

    for entity in ["label", "release-group", "release"] {
        let body = entity_tar_xz(entity, format!("{{\"id\":\"{entity}\"}}\n").as_bytes());
        checksum_lines.push_str(&format!("{} *{}.tar.xz\n", hex::encode(Sha256::digest(&body)), entity));
        let path = format!("/{version}/{entity}.tar.xz");
        let _download = server.mock("GET", path.as_str()).with_status(200).with_body(body).expect(1).create_async().await;
    }
    let _checksums = server
        .mock("GET", format!("/{version}/SHA256SUMS").as_str())
        .with_status(200)
        .with_body(checksum_lines)
        .create_async()
        .await;

    let downloader = MbDownloader::new(temp_dir.path().to_path_buf(), base_url);
    let result = downloader.download_latest().await.unwrap();

    assert!(matches!(result, MbDownloadResult::Downloaded(found) if found == version));
    assert_eq!(std::fs::read(version_dir.join("artist.jsonl.xz")).unwrap(), b"already verified");
    assert!(!version_dir.join("label.jsonl.xz.tmp").exists());
    for entity in ["label", "release-group", "release"] {
        assert!(version_dir.join(format!("{entity}.jsonl.xz")).exists());
    }
}

#[tokio::test]
async fn checksum_or_http_exhaustion_preserves_cause_and_never_publishes() {
    let temp_dir = TempDir::new().unwrap();
    let out_path = temp_dir.path().join("artist.jsonl.xz");
    let body = entity_tar_xz("artist", b"{\"id\":\"wrong\"}\n");
    let mut server = mockito::Server::new_async().await;
    let mismatch = server
        .mock("GET", "/artist.tar.xz")
        .with_status(200)
        .with_body(body)
        .expect(MB_MAX_DOWNLOAD_RETRIES as usize)
        .create_async()
        .await;
    let downloader = MbDownloader::new(temp_dir.path().to_path_buf(), server.url());

    let error = downloader
        .stream_download_verify_extract(&format!("{}/artist.tar.xz", server.url()), "artist", &"0".repeat(64), &out_path)
        .await
        .unwrap_err();
    let chain = format!("{error:#}");

    mismatch.assert_async().await;
    assert!(chain.contains("SHA256 mismatch for artist"), "missing integrity root cause: {chain}");
    assert!(chain.contains("expected") && chain.contains("got"), "checksum error must retain both digests: {chain}");
    assert!(!out_path.exists());
    assert!(!tmp_download_path(&out_path).exists());

    let http_failure = server.mock("GET", "/label.tar.xz").with_status(500).expect(MB_MAX_DOWNLOAD_RETRIES as usize).create_async().await;
    let label_out = temp_dir.path().join("label.jsonl.xz");
    let http_error = downloader
        .stream_download_verify_extract(&format!("{}/label.tar.xz", server.url()), "label", &"0".repeat(64), &label_out)
        .await
        .unwrap_err();

    http_failure.assert_async().await;
    let http_chain = format!("{http_error:#}");
    assert!(http_chain.contains("HTTP error downloading label"), "missing entity context: {http_chain}");
    assert!(http_chain.contains("500 Internal Server Error"), "missing HTTP root cause: {http_chain}");
}
