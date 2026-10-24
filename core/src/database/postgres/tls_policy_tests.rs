//! Observe connection policy at the wire while real PostgreSQL handles successful sessions.
//! The loopback proxy injects negotiation failures; it never serves application queries.
use std::{
    io::{self, Cursor},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use rustls::{pki_types::PrivateKeyDer, ServerConfig, ServerConnection};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_postgres::Config;

use super::{PostgresClient, TEST_DATABASE_URL_LOCK};

const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

#[derive(Clone, Copy)]
enum ServerBehavior {
    DeclineTls,
    DisconnectDuringHandshake,
    UntrustedCertificate,
    RejectAuthentication,
}

enum ConnectionString {
    Uri(&'static str),
    Keyword(&'static str),
}

enum ExpectedOutcome {
    Rejected { plaintext_startups: usize },
    Connected { negotiates_tls: bool },
}

#[derive(Default)]
struct Attempts {
    connections: AtomicUsize,
    ssl_requests: AtomicUsize,
    plaintext_startups: AtomicUsize,
}

struct Proxy {
    port: u16,
    attempts: Arc<Attempts>,
    task: JoinHandle<()>,
}

impl Proxy {
    async fn start(upstream: &url::Url, behavior: ServerBehavior) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind loopback proxy");
        let port = listener.local_addr().expect("proxy address").port();
        let host = upstream.host_str().expect("fixture host").to_string();
        let upstream_port = upstream.port().unwrap_or(5432);
        let attempts = Arc::new(Attempts::default());
        let observed = attempts.clone();
        let task = tokio::spawn(async move {
            // The test fixes the pool size at one, so sessions can be relayed in order.
            loop {
                let (stream, _) = listener.accept().await.expect("accept PostgreSQL connection");
                observed.connections.fetch_add(1, Ordering::SeqCst);
                timeout(
                    Duration::from_secs(15),
                    relay(stream, &host, upstream_port, behavior, &observed),
                )
                .await
                .expect("proxy session completes")
                .expect("proxy protocol exchange succeeds");
            }
        });
        Self { port, attempts, task }
    }

    async fn stop(mut self) -> (usize, usize, usize) {
        self.task.abort();
        let error = (&mut self.task).await.expect_err("proxy runs until stopped");
        assert!(error.is_cancelled(), "proxy task failed: {error}");
        (
            self.attempts.connections.load(Ordering::SeqCst),
            self.attempts.ssl_requests.load(Ordering::SeqCst),
            self.attempts.plaintext_startups.load(Ordering::SeqCst),
        )
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A required-TLS client closes after N; that is an expected end of negotiation.
async fn startup_header(stream: &mut TcpStream) -> io::Result<Option<[u8; 8]>> {
    let mut header = [0; 8];
    match stream.read_exact(&mut header).await {
        Ok(_) => Ok(Some(header)),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(error),
    }
}

async fn relay(
    mut stream: TcpStream,
    host: &str,
    port: u16,
    behavior: ServerBehavior,
    attempts: &Attempts,
) -> io::Result<()> {
    let Some(mut header) = startup_header(&mut stream).await? else { return Ok(()) };
    if header == SSL_REQUEST {
        attempts.ssl_requests.fetch_add(1, Ordering::SeqCst);
        match behavior {
            ServerBehavior::DisconnectDuringHandshake => {
                stream.write_all(b"S").await?;
                assert_eq!(stream.read_u8().await?, 0x16, "client sends a TLS handshake");
                return Ok(());
            }
            ServerBehavior::UntrustedCertificate => {
                stream.write_all(b"S").await?;
                return present_untrusted_certificate(&mut stream).await;
            }
            ServerBehavior::DeclineTls | ServerBehavior::RejectAuthentication => {
                stream.write_all(b"N").await?;
                let Some(startup) = startup_header(&mut stream).await? else { return Ok(()) };
                header = startup;
            }
        }
    }
    assert_eq!(&header[4..], &[0, 3, 0, 0], "PostgreSQL startup packet");
    attempts.plaintext_startups.fetch_add(1, Ordering::SeqCst);
    if matches!(behavior, ServerBehavior::RejectAuthentication) {
        let length = u32::from_be_bytes(header[..4].try_into().expect("startup length"));
        let mut parameters = vec![0; length as usize - header.len()];
        stream.read_exact(&mut parameters).await?;
        // PostgreSQL ErrorResponse: deterministic authentication failure before ReadyForQuery.
        let fields = b"SERROR\0C28P01\0Minjected authentication failure\0\0";
        stream.write_u8(b'E').await?;
        stream.write_u32((fields.len() + 4) as u32).await?;
        stream.write_all(fields).await?;
        return Ok(());
    }
    let mut upstream = TcpStream::connect((host, port)).await?;
    upstream.write_all(&header).await?;
    match tokio::io::copy_bidirectional(&mut stream, &mut upstream).await {
        Ok(_) => Ok(()),
        // Dropping a client or its pool can reset an otherwise completed session.
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => Ok(()),
        Err(error) => Err(error),
    }
}

/// Complete enough of a real TLS handshake for the client to reject the fixture issuer.
/// The committed private key belongs only to this self-signed test certificate.
async fn present_untrusted_certificate(stream: &mut TcpStream) -> io::Result<()> {
    let certificate = include_bytes!("testdata/tls/server.der").to_vec().into();
    let key = PrivateKeyDer::Pkcs8(include_bytes!("testdata/tls/server-key.der").to_vec().into());
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], key)
        .expect("valid test certificate and key");
    let mut server = ServerConnection::new(Arc::new(config)).expect("TLS server");
    let mut input = [0; 8192];
    loop {
        while server.wants_write() {
            let mut output = Vec::new();
            server.write_tls(&mut output)?;
            stream.write_all(&output).await?;
        }
        let received = stream.read(&mut input).await?;
        if received == 0 {
            return Ok(());
        }
        server.read_tls(&mut Cursor::new(&input[..received]))?;
        if server.process_new_packets().is_err() {
            return Ok(());
        }
        assert!(server.is_handshaking(), "client must reject the untrusted fixture certificate");
    }
}

async fn check_policy(
    behavior: ServerBehavior,
    connection_string: ConnectionString,
    expected: ExpectedOutcome,
) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let (mut url, _container) = match std::env::var("RINDEXER_TEST_POSTGRES_URL") {
        Ok(url) => {
            let config: Config = url.parse().expect("valid fixture configuration");
            assert_eq!(config.get_dbname(), Some("rindexer_fixture_admin"));
            (url::Url::parse(&url).expect("fixture URI"), None)
        }
        Err(std::env::VarError::NotPresent) => {
            let container = Postgres::default().start().await.expect("start PostgreSQL fixture");
            let host = container.get_host().await.expect("fixture host");
            let port = container.get_host_port_ipv4(5432).await.expect("fixture port");
            let url = format!("postgresql://postgres:postgres@{host}:{port}/postgres");
            (url::Url::parse(&url).expect("native fixture URI"), Some(container))
        }
        Err(error) => panic!("invalid fixture URL: {error}"),
    };
    let proxy = Proxy::start(&url, behavior).await;
    url.set_host(Some("127.0.0.1")).expect("proxy host");
    url.set_port(Some(proxy.port)).expect("proxy port");
    url.set_query(None);
    let connection_string = match connection_string {
        // Preserve percent-encoded sslmode spellings rather than normalizing the input.
        ConnectionString::Uri(mode) => format!("{url}?sslmode={mode}&connect_timeout=2"),
        ConnectionString::Keyword(mode) => {
            // These tests use only the fixed disposable fixture credentials.
            assert_eq!(url.username(), "postgres");
            assert_eq!(url.password(), Some("postgres"));
            format!(
                "host=127.0.0.1 port={} user=postgres password=postgres dbname={} connect_timeout=2 {mode}",
                proxy.port,
                url.path().trim_start_matches('/')
            )
        }
    };
    let _guard = TEST_DATABASE_URL_LOCK.lock().await;
    let old_url = std::env::var_os("DATABASE_URL");
    let old_pool_size = std::env::var_os("DATABASE_POOL_SIZE");
    std::env::set_var("DATABASE_URL", &connection_string);
    std::env::set_var("DATABASE_POOL_SIZE", "1");
    let result = timeout(Duration::from_secs(10), async {
        let postgres = PostgresClient::new().await?;
        let row = postgres.query_one("SELECT 1", &[]).await?;
        anyhow::ensure!(row.get::<_, i32>(0) == 1, "query through pooled connection succeeds");
        Ok::<_, anyhow::Error>(())
    })
    .await;
    for (name, original) in [("DATABASE_URL", old_url), ("DATABASE_POOL_SIZE", old_pool_size)] {
        match original {
            Some(value) => std::env::set_var(name, value),
            None => std::env::remove_var(name),
        }
    }
    let (connections, ssl_requests, plaintext_startups) = proxy.stop().await;
    let result = result.expect("PostgreSQL policy check completes within ten seconds");
    match expected {
        ExpectedOutcome::Rejected { plaintext_startups: expected_plaintext } => {
            assert!(result.is_err(), "connection must fail rather than reconnect in plaintext");
            assert_eq!(connections, 1, "application must not retry the failed connection");
            assert_eq!(ssl_requests, 1);
            assert_eq!(plaintext_startups, expected_plaintext);
        }
        ExpectedOutcome::Connected { negotiates_tls } => {
            result.expect("permitted plaintext connection succeeds");
            assert!(connections >= 2, "exercise both startup probe and connection pool");
            assert_eq!(plaintext_startups, connections);
            assert_eq!(ssl_requests, if negotiates_tls { connections } else { 0 });
        }
    }
}

#[tokio::test]
async fn uri_require_rejects_a_plaintext_server() {
    check_policy(
        ServerBehavior::DeclineTls,
        ConnectionString::Uri("require"),
        ExpectedOutcome::Rejected { plaintext_startups: 0 },
    )
    .await;
}

#[tokio::test]
async fn keyword_require_rejects_a_plaintext_server() {
    check_policy(
        ServerBehavior::DeclineTls,
        ConnectionString::Keyword("sslmode = 'require'"),
        ExpectedOutcome::Rejected { plaintext_startups: 0 },
    )
    .await;
}

#[tokio::test]
async fn encoded_uri_require_rejects_a_plaintext_server() {
    check_policy(
        ServerBehavior::DeclineTls,
        ConnectionString::Uri("%72equire"),
        ExpectedOutcome::Rejected { plaintext_startups: 0 },
    )
    .await;
}

#[tokio::test]
async fn prefer_accepts_plaintext_when_server_declines_tls() {
    check_policy(
        ServerBehavior::DeclineTls,
        ConnectionString::Uri("prefer"),
        ExpectedOutcome::Connected { negotiates_tls: true },
    )
    .await;
}

#[tokio::test]
async fn disable_connects_without_tls_negotiation() {
    check_policy(
        ServerBehavior::DeclineTls,
        ConnectionString::Uri("disable"),
        ExpectedOutcome::Connected { negotiates_tls: false },
    )
    .await;
}

#[tokio::test]
async fn prefer_does_not_reconnect_after_handshake_disconnect() {
    check_policy(
        ServerBehavior::DisconnectDuringHandshake,
        ConnectionString::Uri("prefer"),
        ExpectedOutcome::Rejected { plaintext_startups: 0 },
    )
    .await;
}

#[tokio::test]
async fn prefer_does_not_reconnect_after_certificate_rejection() {
    check_policy(
        ServerBehavior::UntrustedCertificate,
        ConnectionString::Uri("prefer"),
        ExpectedOutcome::Rejected { plaintext_startups: 0 },
    )
    .await;
}

#[tokio::test]
async fn require_rejects_an_untrusted_certificate() {
    check_policy(
        ServerBehavior::UntrustedCertificate,
        ConnectionString::Uri("require"),
        ExpectedOutcome::Rejected { plaintext_startups: 0 },
    )
    .await;
}

#[tokio::test]
async fn prefer_does_not_retry_authentication_failure_without_tls() {
    check_policy(
        ServerBehavior::RejectAuthentication,
        ConnectionString::Uri("prefer"),
        ExpectedOutcome::Rejected { plaintext_startups: 1 },
    )
    .await;
}
