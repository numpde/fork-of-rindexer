use bb8::{Pool, RunError};
use bb8_lapin::LapinConnectionManager;
use lapin::{
    options::*, types::FieldTable, BasicProperties, DefaultConnectionBuilder, ExchangeKind,
};
use serde_json::Value;

use crate::manifest::stream::ExchangeKindWrapper;
use crate::streams::publish_with_retry;

#[derive(thiserror::Error, Debug)]
pub enum RabbitMQError {
    #[error("Request error: {0}")]
    LapinError(#[from] lapin::Error),

    #[error("Could not parse message: {0}")]
    CouldNotParseMessage(#[from] serde_json::Error),

    #[error("Connection pool timed out")]
    PoolTimedOut,
}

impl From<RunError<lapin::ErrorKind>> for RabbitMQError {
    fn from(err: RunError<lapin::ErrorKind>) -> Self {
        match err {
            RunError::User(kind) => Self::LapinError(lapin::Error::from(kind)),
            RunError::TimedOut => Self::PoolTimedOut,
        }
    }
}

// TODO: drop the direct `async-rs` dep once bb8-lapin/lapin re-export
// `DefaultRuntimeKit` publicly. We only depend on async-rs to name
// `async_rs::Tokio` here; `lapin::runtime::DefaultRuntimeKit` is `pub(crate)`.
#[derive(Debug, Clone)]
pub struct RabbitMQ {
    pool: Pool<LapinConnectionManager<async_rs::Tokio>>,
}

// Compile-time guarantee that `RabbitMQ` stays Send + Sync. A future bb8-lapin
// trait-bound regression on the pool generic would surface here at build time
// instead of at the first concurrent publish in production.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<RabbitMQ>();
};

impl RabbitMQ {
    pub async fn new(uri: &str) -> Result<Self, RabbitMQError> {
        let builder = DefaultConnectionBuilder::new()
            .map_err(RabbitMQError::LapinError)?
            .with_uri_str(uri.to_string());
        let manager = LapinConnectionManager::new(builder);
        let pool = Pool::builder()
            .max_size(16)
            .build(manager)
            .await
            .map_err(|kind| RabbitMQError::LapinError(lapin::Error::from(kind)))?;

        Ok(Self { pool })
    }

    pub async fn publish(
        &self,
        id: &str,
        exchange: &str,
        exchange_type: &ExchangeKindWrapper,
        routing_key: &Option<String>,
        message: &Value,
    ) -> Result<(), RabbitMQError> {
        publish_with_retry("rabbitmq", exchange, || {
            self.publish_once(id, exchange, exchange_type, routing_key, message)
        })
        .await
    }

    async fn publish_once(
        &self,
        id: &str,
        exchange: &str,
        exchange_type: &ExchangeKindWrapper,
        routing_key: &Option<String>,
        message: &Value,
    ) -> Result<(), RabbitMQError> {
        let message_body = serde_json::to_vec(message)?;

        let conn = self.pool.get().await?;
        let channel = conn.create_channel().await?;

        channel
            .exchange_declare(
                exchange.into(),
                exchange_type.0.clone(),
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let routing_key: &str = match exchange_type.0 {
            ExchangeKind::Fanout => "", // Fanout exchange ignores the routing key
            _ => routing_key.as_deref().expect("Routing key should be defined"),
        };

        channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                &message_body,
                BasicProperties::default()
                    .with_message_id(id.into())
                    .with_content_type("application/json".into()),
            )
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streams::rabbitmq_test_broker::{BrokerReply, TestBroker};
    use std::time::Duration;

    async fn publish_once(
        broker: &TestBroker,
    ) -> tokio::task::JoinHandle<Result<(), RabbitMQError>> {
        let client = RabbitMQ::new(&broker.url).await.unwrap();
        tokio::spawn(async move {
            client
                .publish_once(
                    "durable-delivery-id",
                    "events",
                    &ExchangeKindWrapper(ExchangeKind::Direct),
                    &Some("values".into()),
                    &serde_json::json!({"value": 7}),
                )
                .await
        })
    }

    #[tokio::test]
    async fn rabbitmq_publish_waits_for_broker_ack() {
        let mut broker = TestBroker::start().await;
        let mut publishing = publish_once(&broker).await;
        let pending = tokio::time::timeout(Duration::from_secs(5), broker.publishes.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(serde_json::from_slice::<Value>(&pending.body).unwrap()["value"], 7);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut publishing).await.is_err(),
            "a socket write must not acknowledge a delivery before the broker confirms it"
        );
        assert!(pending.confirms_selected, "publisher confirmations must be enabled");
        pending.reply.send(BrokerReply::Ack).unwrap_or_else(|_| panic!("broker stopped"));
        tokio::time::timeout(Duration::from_secs(5), publishing).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn rabbitmq_publish_rejects_broker_nack() {
        let mut broker = TestBroker::start().await;
        let publishing = publish_once(&broker).await;
        let pending = tokio::time::timeout(Duration::from_secs(5), broker.publishes.recv())
            .await
            .unwrap()
            .unwrap();
        pending.reply.send(BrokerReply::Nack).unwrap_or_else(|_| panic!("broker stopped"));
        let result =
            tokio::time::timeout(Duration::from_secs(5), publishing).await.unwrap().unwrap();
        let error = result.expect_err("a broker rejection must retain the delivery for retry");
        assert_eq!(error.to_string(), "RabbitMQ broker rejected the publication");
    }

    #[tokio::test]
    async fn rabbitmq_publish_rejects_disconnect_before_confirmation() {
        let mut broker = TestBroker::start().await;
        let publishing = publish_once(&broker).await;
        let pending = tokio::time::timeout(Duration::from_secs(5), broker.publishes.recv())
            .await
            .unwrap()
            .unwrap();
        pending.reply.send(BrokerReply::Disconnect).unwrap_or_else(|_| panic!("broker stopped"));
        let result =
            tokio::time::timeout(Duration::from_secs(5), publishing).await.unwrap().unwrap();
        assert!(result.is_err(), "a write without a broker confirmation is an uncertain delivery");
    }
}
