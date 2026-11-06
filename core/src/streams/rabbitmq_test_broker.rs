//! A small AMQP 0-9-1 peer that controls broker confirmations independently of
//! socket writes. Tests still use the production Lapin connection and publisher.
use std::collections::{HashMap, HashSet};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

pub(crate) enum BrokerReply {
    Ack,
    Nack,
    Disconnect,
}

pub(crate) struct PendingPublish {
    pub body: Vec<u8>,
    pub confirms_selected: bool,
    pub reply: oneshot::Sender<BrokerReply>,
}

pub(crate) struct TestBroker {
    pub url: String,
    pub publishes: mpsc::UnboundedReceiver<PendingPublish>,
    server: JoinHandle<()>,
}

impl TestBroker {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("amqp://guest:guest@{}/%2f", listener.local_addr().unwrap());
        let (sender, publishes) = mpsc::unbounded_channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = [0; 8];
            socket.read_exact(&mut header).await.unwrap();
            assert_eq!(&header, b"AMQP\0\0\x09\x01");

            // connection.start: version, empty server properties, mechanisms, locales.
            let mut start = vec![0, 9, 0, 0, 0, 0];
            for value in [b"PLAIN".as_slice(), b"en_US".as_slice()] {
                start.extend_from_slice(&(value.len() as u32).to_be_bytes());
                start.extend_from_slice(value);
            }
            method(&mut socket, 0, 10, 10, &start).await;
            expect_method(&mut socket, 10, 11).await;
            // connection.tune: 2047 channels, 128 KiB frames, no heartbeat.
            method(&mut socket, 0, 10, 30, &[0x07, 0xff, 0, 2, 0, 0, 0, 0]).await;
            expect_method(&mut socket, 10, 31).await;
            expect_method(&mut socket, 10, 40).await;
            method(&mut socket, 0, 10, 41, &[0]).await;

            let mut confirms = HashSet::new();
            let mut bodies: HashMap<u16, (usize, Vec<u8>)> = HashMap::new();
            let mut delivery_tags: HashMap<u16, u64> = HashMap::new();
            loop {
                let Some((kind, channel, payload)) = frame(&mut socket).await else { return };
                match kind {
                    1 => {
                        let class = u16::from_be_bytes(payload[0..2].try_into().unwrap());
                        let id = u16::from_be_bytes(payload[2..4].try_into().unwrap());
                        match (class, id) {
                            (20, 10) => method(&mut socket, channel, 20, 11, &[0; 4]).await,
                            (40, 10) => method(&mut socket, channel, 40, 11, &[]).await,
                            (85, 10) => {
                                confirms.insert(channel);
                                method(&mut socket, channel, 85, 11, &[]).await;
                            }
                            (60, 40) => {} // basic.publish; header/body frames follow.
                            (20, 40) => method(&mut socket, channel, 20, 41, &[]).await,
                            (10, 50) => {
                                method(&mut socket, 0, 10, 51, &[]).await;
                                return;
                            }
                            other => panic!("unexpected AMQP method {other:?}"),
                        }
                    }
                    2 => {
                        let size = u64::from_be_bytes(payload[4..12].try_into().unwrap());
                        bodies.insert(channel, (usize::try_from(size).unwrap(), Vec::new()));
                    }
                    3 => {
                        let (size, body) = bodies.get_mut(&channel).unwrap();
                        body.extend_from_slice(&payload);
                        assert!(body.len() <= *size);
                        if body.len() != *size {
                            continue;
                        }
                        let (_, body) = bodies.remove(&channel).unwrap();
                        let (reply, response) = oneshot::channel();
                        if sender
                            .send(PendingPublish {
                                body,
                                confirms_selected: confirms.contains(&channel),
                                reply,
                            })
                            .is_err()
                        {
                            return;
                        }
                        let response = match response.await {
                            Ok(BrokerReply::Ack) => 80,
                            Ok(BrokerReply::Nack) => 120,
                            Ok(BrokerReply::Disconnect) | Err(_) => return,
                        };
                        let tag = delivery_tags.entry(channel).or_default();
                        *tag += 1;
                        let mut fields = tag.to_be_bytes().to_vec();
                        fields.push(0); // multiple=false; requeue=false for nack.
                        method(&mut socket, channel, 60, response, &fields).await;
                    }
                    8 => {} // heartbeat
                    other => panic!("unexpected AMQP frame type {other}"),
                }
            }
        });
        Self { url, publishes, server }
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn frame(socket: &mut TcpStream) -> Option<(u8, u16, Vec<u8>)> {
    let mut header = [0; 7];
    if socket.read_exact(&mut header).await.is_err() {
        return None;
    }
    let channel = u16::from_be_bytes(header[1..3].try_into().unwrap());
    let size = u32::from_be_bytes(header[3..7].try_into().unwrap()) as usize;
    assert!(size <= 131_072, "test peer received an oversized frame");
    let mut payload = vec![0; size];
    socket.read_exact(&mut payload).await.unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), 0xce);
    Some((header[0], channel, payload))
}

async fn expect_method(socket: &mut TcpStream, class: u16, id: u16) {
    let (kind, channel, payload) = frame(socket).await.unwrap();
    assert_eq!((kind, channel), (1, 0));
    assert_eq!(u16::from_be_bytes(payload[0..2].try_into().unwrap()), class);
    assert_eq!(u16::from_be_bytes(payload[2..4].try_into().unwrap()), id);
}

async fn method(socket: &mut TcpStream, channel: u16, class: u16, id: u16, fields: &[u8]) {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&channel.to_be_bytes());
    bytes.extend_from_slice(&((fields.len() + 4) as u32).to_be_bytes());
    bytes.extend_from_slice(&class.to_be_bytes());
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend_from_slice(fields);
    bytes.push(0xce);
    socket.write_all(&bytes).await.unwrap();
}
