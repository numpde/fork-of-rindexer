//! A test-only PostgreSQL wire fault that withholds the COMMIT reply.
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

pub(crate) struct LostCommitResponse {
    pub(crate) url: url::Url,
    pub(crate) dropped: Arc<AtomicBool>,
    task: JoinHandle<()>,
}

impl LostCommitResponse {
    pub(crate) async fn start(upstream: &url::Url) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let host = upstream.host_str().unwrap().to_string();
        let port = upstream.port().unwrap_or(5432);
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (incoming, _) = accepted.unwrap();
                        let host = host.clone();
                        let dropped = observed.clone();
                        sessions.spawn(async move {
                            let upstream = TcpStream::connect((host.as_str(), port)).await.unwrap();
                            // A dropped session is the injected condition; protocol errors
                            // still fail the test through this JoinSet's result.
                            relay(incoming, upstream, dropped).await
                        });
                    }
                    Some(joined) = sessions.join_next(), if !sessions.is_empty() => {
                        joined.unwrap().unwrap();
                    }
                }
            }
        });
        let mut url = upstream.clone();
        url.set_host(Some("127.0.0.1")).unwrap();
        url.set_port(Some(address.port())).unwrap();
        Self { url, dropped, task }
    }

    pub(crate) async fn stop(mut self) {
        self.task.abort();
        let error = (&mut self.task).await.expect_err("proxy runs until explicitly stopped");
        assert!(error.is_cancelled(), "proxy session failed: {error}");
    }
}

impl Drop for LostCommitResponse {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn relay(
    mut incoming: TcpStream,
    mut upstream: TcpStream,
    dropped: Arc<AtomicBool>,
) -> io::Result<()> {
    // The fixture client explicitly disables TLS. Only the startup packet lacks
    // the usual one-byte message tag; all following messages are framed.
    let size = incoming.read_u32().await?;
    if !(8..=1_048_576).contains(&size) {
        return Err(io::Error::other("invalid startup frame"));
    }
    let mut startup = vec![0; size as usize - 4];
    incoming.read_exact(&mut startup).await?;
    upstream.write_u32(size).await?;
    upstream.write_all(&startup).await?;
    let (mut client_read, mut client_write) = incoming.into_split();
    let (mut server_read, mut server_write) = upstream.into_split();
    let forward_requests = tokio::io::copy(&mut client_read, &mut server_write);
    let forward_responses = async {
        loop {
            let tag = match server_read.read_u8().await {
                Ok(tag) => tag,
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error),
            };
            let size = server_read.read_u32().await?;
            if !(4..=1_048_576).contains(&size) {
                return Err(io::Error::other("invalid backend frame"));
            }
            let mut body = vec![0; size as usize - 4];
            server_read.read_exact(&mut body).await?;
            if tag == b'C' && body == b"COMMIT\0" && !dropped.swap(true, Ordering::SeqCst) {
                return Ok(()); // PostgreSQL committed; the client receives neither C nor Z.
            }
            client_write.write_u8(tag).await?;
            client_write.write_u32(size).await?;
            client_write.write_all(&body).await?;
        }
    };
    tokio::select! {
        result = forward_requests => result.map(|_| ()),
        result = forward_responses => result,
    }
}
