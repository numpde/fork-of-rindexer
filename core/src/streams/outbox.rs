//! Durable routing shares the legacy destination selection and publisher adapters.
use super::*;
use crate::database::postgres::{
    client::PostgresClient,
    stream_outbox::{self, PendingDelivery},
};
use crate::event::callback_registry::{CallbackFailures, FailedStream, TxInformation};
use tokio_util::sync::CancellationToken;

impl StreamsClients {
    pub(crate) fn requires_durable_delivery(&self) -> bool {
        self.delivery_guarantee == StreamDeliveryGuarantee::AtLeastOnce
    }

    pub(crate) fn bind_delivery_source(
        &self,
        source: DeliverySource,
        eligible: bool,
    ) -> Result<(), String> {
        let binding = self.delivery_source.get_or_init(|| {
            if eligible { Ok(source.clone()) } else {
                Err("Cannot require stream delivery without PostgreSQL-only no-code contract events and retained raw-event/application receipts".to_string())
            }
        });
        match binding {
            Ok(existing) if existing == &source => Ok(()),
            Ok(_) => {
                Err("Cannot share a required-delivery client between different source contracts"
                    .to_string())
            }
            Err(error) => Err(error.clone()),
        }
    }

    pub(crate) fn delivery_source(&self) -> anyhow::Result<&DeliverySource> {
        self.delivery_source
            .get()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Required stream delivery needs a no-code PostgreSQL source binding"
                )
            })?
            .as_ref()
            .map_err(|error| anyhow::anyhow!("{error}"))
    }

    pub(super) fn matched_destinations(
        &self,
        event: &EventMessage,
        trace: bool,
        force: bool,
    ) -> Vec<DeliveryDestination> {
        let mut out = Vec::new();
        let mut append = |label: &str,
                          index: usize,
                          events: &[StreamEvent],
                          networks: &[String],
                          delivery: Option<&StreamDeliveryMode>,
                          identity: Value| {
            if networks.contains(&event.network)
                && Self::should_send_for_config(events, &event.event_name, trace, force)
            {
                out.push(DeliveryDestination {
                    stream_type: label.to_string(),
                    config_index: index,
                    fingerprint: format!(
                        "{:#x}",
                        alloy::primitives::keccak256(
                            serde_json::to_vec(&(label, identity))
                                .expect("JSON routing identity serializes")
                        )
                    ),
                    finalized: delivery == Some(&StreamDeliveryMode::Finalized),
                });
            }
        };
        if let Some(stream) = &self.sns {
            for (index, config) in stream.config.iter().enumerate() {
                append(
                    stream_type::SNS,
                    index,
                    &config.events,
                    &config.networks,
                    config.delivery.as_ref(),
                    json!(config),
                );
            }
        }
        if let Some(stream) = &self.webhook {
            for (index, config) in stream.config.iter().enumerate() {
                let mut identity = json!(config);
                identity
                    .as_object_mut()
                    .expect("webhook config is an object")
                    .remove("shared_secret");
                identity["endpoint"] = json!(connection_identity(&config.endpoint));
                append(
                    stream_type::WEBHOOK,
                    index,
                    &config.events,
                    &config.networks,
                    config.delivery.as_ref(),
                    identity,
                );
            }
        }
        if let Some(stream) = &self.rabbitmq {
            for (index, config) in stream.config.exchanges.iter().enumerate() {
                append(
                    stream_type::RABBITMQ,
                    index,
                    &config.events,
                    &config.networks,
                    config.delivery.as_ref(),
                    json!({"target":connection_identity(&stream.config.url),"config":config}),
                );
            }
        }
        #[cfg(feature = "kafka")]
        if let Some(stream) = &self.kafka {
            for (index, config) in stream.config.topics.iter().enumerate() {
                append(
                    stream_type::KAFKA,
                    index,
                    &config.events,
                    &config.networks,
                    config.delivery.as_ref(),
                    json!({"brokers":stream.config.brokers,"config":config}),
                );
            }
        }
        if let Some(stream) = &self.redis {
            for (index, config) in stream.config.streams.iter().enumerate() {
                append(
                    stream_type::REDIS,
                    index,
                    &config.events,
                    &config.networks,
                    config.delivery.as_ref(),
                    json!({"target":connection_identity(&stream.config.connection_uri),"config":config}),
                );
            }
        }
        if let Some(stream) = &self.cloudflare_queues {
            for (index, config) in stream.config.queues.iter().enumerate() {
                append(
                    stream_type::CLOUDFLARE_QUEUES,
                    index,
                    &config.events,
                    &config.networks,
                    config.delivery.as_ref(),
                    json!({"account":stream.config.account_id,"config":config}),
                );
            }
        }
        out
    }

    pub(crate) fn network_destinations(&self, network: &str) -> Vec<DeliveryDestination> {
        self.matched_destinations(
            &EventMessage {
                event_name: "__rindexer_reorg".into(),
                event_data: json!([]),
                event_signature_hash: B256::ZERO,
                network: network.to_string(),
                block_number: 0,
            },
            false,
            true,
        )
    }

    pub(crate) async fn enqueue_required_event(
        &self,
        tx: &tokio_postgres::Transaction<'_>,
        source_table: &str,
        tx_info: &TxInformation,
        message: &EventMessage,
    ) -> anyhow::Result<()> {
        let source = self.delivery_source()?;
        for destination in self.matched_destinations(message, false, false) {
            if destination.finalized {
                let distance=*self.reorg_safe_distances.lock().unwrap_or_else(|error|error.into_inner()).get(&message.network)
                    .ok_or_else(|| anyhow::anyhow!("Cannot schedule required finalized delivery without a registered recovery distance"))?;
                tx_info.block_number.checked_add(distance).ok_or_else(|| {
                    anyhow::anyhow!(
                        "Cannot schedule required finalized delivery beyond the block-number range"
                    )
                })?;
            }
            stream_outbox::enqueue(
                tx,
                PendingDelivery {
                    id: crate::helpers::generate_random_id(32),
                    source,
                    source_table: Some(source_table),
                    block_number: tx_info.block_number,
                    destination: &destination,
                    message,
                },
            )
            .await?;
        }
        Ok(())
    }
}

/// Authentication rotation does not retarget a queued obligation. Routing hosts,
/// paths, exchanges/topics and event policies remain part of its fingerprint.
fn connection_identity(connection: &str) -> String {
    let Ok(mut url) = url::Url::parse(connection) else { return connection.to_string() };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.to_string()
}

static DELIVERY_LEASE: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(1));

impl StreamsClients {
    pub(crate) fn start_delivery_worker(
        clients: &Arc<Option<Self>>,
        postgres: Arc<PostgresClient>,
        network: String,
        cancel: CancellationToken,
        failures: Arc<CallbackFailures>,
        identity: FailedStream,
    ) {
        use futures::FutureExt;
        let Some(client) = clients.as_ref() else { return };
        let mut workers = client.delivery_workers.lock().unwrap_or_else(|error| error.into_inner());
        if workers
            .get(&network)
            .is_some_and(|(token, handle)| !token.is_cancelled() && !handle.is_finished())
        {
            return;
        }
        let weak = Arc::downgrade(clients);
        let worker_cancel = cancel.clone();
        let worker_network = network.clone();
        let handle = tokio::spawn(async move {
            let network = worker_network;
            let run = async {
                while crate::is_running() && !worker_cancel.is_cancelled() {
                    let Some(owner) = weak.upgrade() else { break };
                    let Some(client) = owner.as_ref() else { break };
                    let epoch = crate::indexer::reorg::callback_recovery_token(&network);
                    let barrier = crate::indexer::reorg::event_writer_barrier(&network);
                    let attempt = async {
                        let _lease =
                            DELIVERY_LEASE.acquire().await.expect("delivery lease stays open");
                        let _guard = barrier.read().await;
                        // Admission may wait behind another worker or recovery. Once
                        // admitted, reserve the last five seconds for durable deferral
                        // or acknowledgement after the publisher wait ends.
                        let deadline =
                            tokio::time::Instant::now() + std::time::Duration::from_secs(30);
                        let publish_deadline = deadline - std::time::Duration::from_secs(5);
                        tokio::time::timeout_at(deadline, client.deliver_one(&postgres, &network, publish_deadline))
                            .await
                            .unwrap_or_else(|_| {
                                Err(anyhow::anyhow!(
                                    "Required delivery attempt timed out before confirmation; durable PostgreSQL state will determine retry"
                                ))
                            })
                    };
                    let result = tokio::select! {
                        _=worker_cancel.cancelled()=>break,
                        _=epoch.cancelled()=>Ok(false),
                        result=attempt=>result,
                    };
                    drop(owner);
                    let delay = match result {
                        Ok(true) => continue,
                        Ok(false) => std::time::Duration::from_millis(200),
                        Err(error) => {
                            if error.downcast_ref::<tokio::task::JoinError>().is_some() {
                                return Err(error);
                            }
                            tracing::warn!(%network,error=%error,"Required stream delivery was not confirmed; durable PostgreSQL state will determine retry");
                            std::time::Duration::from_secs(1)
                        }
                    };
                    tokio::select! { _=worker_cancel.cancelled()=>break, _=tokio::time::sleep(delay)=>{} }
                }
                Ok::<(), anyhow::Error>(())
            };
            let failure=match std::panic::AssertUnwindSafe(run).catch_unwind().await {
                Ok(Ok(()))=>None,
                Ok(Err(error))=>Some(format!("Required PostgreSQL delivery worker stopped after publisher task failure: {error:#}; pending obligations remain durable")),
                Err(_)=>Some("Required PostgreSQL delivery worker panicked; pending obligations remain durable".to_string()),
            };
            if let Some(reason) = failure {
                failures.record_stream(identity, reason.clone());
                tracing::error!("{reason}");
            }
        });
        workers.insert(network, (cancel, handle.abort_handle()));
    }

    async fn deliver_one(
        &self,
        postgres: &PostgresClient,
        network: &str,
        publish_deadline: tokio::time::Instant,
    ) -> anyhow::Result<bool> {
        let source = self.delivery_source()?;
        let mut connection = postgres.raw_connection().await?;
        let tx = connection.transaction().await?;
        // Pending records follow the current network policy, including a stricter
        // safe distance registered after they were committed.
        let distance = self
            .reorg_safe_distances
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(network)
            .copied();
        let Some(row) = stream_outbox::next(&tx, source, network, distance).await? else {
            stream_outbox::prune(&tx, network).await?;
            tx.commit().await?;
            return Ok(false);
        };
        let id: String = row.try_get(0)?;
        let destination: DeliveryDestination = serde_json::from_value(row.try_get::<_, Value>(1)?)?;
        let message: EventMessage = serde_json::from_value(row.try_get::<_, Value>(2)?)?;
        anyhow::ensure!(self.network_destinations(network).contains(&destination),
            "Cannot deliver pending record {id}: its configured destination changed; restore the original routing configuration before retrying");
        let events = message
            .event_data
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Required delivery record {id} has no event array"))?;
        // Aborting this attempt must also abort publisher tasks; dropping ordinary
        // JoinHandles would detach their retries into the next indexing generation.
        let tasks = self.destination_tasks(
            &destination.stream_type,
            destination.config_index,
            &id,
            &message,
            Arc::new(self.chunk_data(events)),
            message.event_name == "__rindexer_reorg",
        );
        let tasks =
            tasks.into_iter().map(tokio_util::task::AbortOnDropHandle::new).collect::<Vec<_>>();
        let published = tokio::time::timeout_at(publish_deadline, async {
            for result in futures::future::join_all(tasks).await {
                result??;
            }
            Ok::<(), anyhow::Error>(())
        })
        .await
        .unwrap_or_else(|_| Err(anyhow::anyhow!(
            "Required stream publisher for record {id} exceeded its deadline; destination confirmation is unknown"
        )));
        if let Err(error) = published {
            stream_outbox::defer(&tx, &id).await?;
            tx.commit().await?;
            return Err(error);
        }
        stream_outbox::acknowledge(&tx, &id).await?;
        stream_outbox::prune(&tx, network).await?;
        tx.commit().await?;
        Ok(true)
    }
}

impl Drop for StreamsClients {
    fn drop(&mut self) {
        for (_, handle) in
            self.delivery_workers.get_mut().unwrap_or_else(|error| error.into_inner()).values()
        {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod routing_tests {
    use super::*;
    #[tokio::test]
    async fn delivery_routing_identity_allows_auth_rotation_but_not_retargeting() {
        async fn client(endpoint: &str, secret: &str) -> StreamsClients {
            StreamsClients::new(serde_json::from_value(json!({"delivery_guarantee":"at_least_once","webhooks":[{
                "endpoint":endpoint,"shared_secret":secret,"networks":["ethereum"],"events":[{"event_name":"Value"}]
            }]})).unwrap()).await
        }
        let original = client("http://old:password@localhost:1234/events", "first").await;
        let rotated = client("http://new:credential@localhost:1234/events", "second").await;
        let retargeted = client("http://localhost:1235/events", "second").await;
        assert_eq!(
            original.network_destinations("ethereum"),
            rotated.network_destinations("ethereum")
        );
        assert_ne!(
            original.network_destinations("ethereum"),
            retargeted.network_destinations("ethereum")
        );
        let message = EventMessage {
            event_name: "Value".into(),
            network: "ethereum".into(),
            block_number: 100,
            event_signature_hash: B256::ZERO,
            event_data: json!([{}]),
        };
        assert!(
            original.stream("direct".into(), &message, false, false).await.is_err(),
            "a direct call cannot bypass its required source transaction"
        );
    }
}
