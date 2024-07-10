// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::IndexerConfig;
use crate::metrics::BridgeIndexerMetrics;
use crate::postgres_manager::{
    read_eth_progress_store, update_earliest_block_synced, update_latest_block_synced, write,
    PgPool,
};
use crate::{
    BridgeDataSource, ProcessedTxnData, TokenTransfer, TokenTransferData, TokenTransferStatus,
};
use anyhow::Result;
use ethers::providers::{Http, Middleware, Provider, StreamExt, Ws};
use ethers::types::{Address as EthAddress, Block, Filter, Transaction, H256};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use sui_bridge::abi::{EthBridgeEvent, EthSuiBridgeEvents};
use sui_bridge::error::BridgeError;
use sui_bridge::eth_client::EthClient;
use sui_bridge::metrics::BridgeMetrics;
use sui_bridge::retry_with_max_elapsed_time;
use sui_bridge::types::{EthEvent, RawEthLog};
use tokio::time;
use tracing::log::error;

const MAX_BLOCK_RANGE: u64 = 1000;
const FINALIZED_BLOCK_QUERY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct EthBridgeWorker {
    provider: Arc<Provider<Http>>,
    pg_pool: PgPool,
    bridge_metrics: Arc<BridgeMetrics>,
    metrics: BridgeIndexerMetrics,
    pub bridge_address: EthAddress,
    config: IndexerConfig,
}

impl EthBridgeWorker {
    pub fn new(
        pg_pool: PgPool,
        bridge_metrics: Arc<BridgeMetrics>,
        metrics: BridgeIndexerMetrics,
        config: IndexerConfig,
    ) -> Result<Self, anyhow::Error> {
        let bridge_address = EthAddress::from_str(&config.eth_sui_bridge_contract_address)?;

        let provider = Arc::new(
            Provider::<Http>::try_from(&config.eth_rpc_url)?
                .interval(std::time::Duration::from_millis(2000)),
        );

        Ok(Self {
            provider,
            pg_pool,
            bridge_metrics,
            metrics,
            bridge_address,
            config,
        })
    }

    pub async fn sync_events(
        &self,
        client: Arc<EthClient<ethers::providers::Http>>,
        genesis_block: u64,
    ) {
        println!("Syncing event history");

        // get current block number
        let current_block = self.provider.get_block_number().await.unwrap().as_u64();

        // get eth sync progress store
        let (mut earliest_block_synced, latest_block_synced) =
            read_eth_progress_store(&self.pg_pool).unwrap();

        if earliest_block_synced == 0 {
            earliest_block_synced = current_block;
        }

        // start range from current block to MAX_BLOCK_RANGE blocks back
        let mut end_block = current_block;
        let mut start_block = end_block - MAX_BLOCK_RANGE;

        loop {
            // if next range is before genesis block, break
            if end_block <= genesis_block {
                break;
            }

            // if range overlaps with latest block synced and the end block is still after the latest block synced
            if start_block < latest_block_synced && end_block > latest_block_synced {
                start_block = latest_block_synced;
            }

            let events = retry_with_max_elapsed_time!(
                client.get_raw_events_in_range(self.bridge_address, start_block, end_block),
                Duration::from_secs(30000)
            )
            .and_then(|events_result| events_result)
            .unwrap();

            if events.len() > 0 {
                println!(
                    "⏳ Processing {} events in range {} to {}",
                    events.len(),
                    start_block,
                    end_block
                );
                // TODO: handle error
                let _ = retry_with_max_elapsed_time!(
                    process_eth_events(
                        self.provider.clone(),
                        client.clone(),
                        self.pg_pool.clone(),
                        self.metrics.clone(),
                        events.clone(),
                    ),
                    Duration::from_millis(30000)
                );
                println!(
                    "✅ Processed {} events in range {} to {}",
                    events.len(),
                    start_block,
                    end_block
                );
            }

            // if range connects with latest block synced or first sync, update latest block synced to end block
            if start_block == latest_block_synced
                || (latest_block_synced == 0 && end_block == current_block)
            {
                // TODO: handle error
                println!(
                    "🚀 Caught up to previous sync... skipping to {}",
                    earliest_block_synced
                );
                let _ = update_latest_block_synced(&self.pg_pool.clone(), current_block);
                end_block = earliest_block_synced;
            } else {
                // if range does not connect with latest block synced, grab next range
                end_block = start_block;
            }

            start_block = end_block - MAX_BLOCK_RANGE;

            if start_block <= genesis_block {
                start_block = genesis_block;
            }

            // if range overlaps with earliest block synced update sync progress to include latest block to current block
            if start_block < earliest_block_synced {
                // TODO: handle error
                let _ = update_earliest_block_synced(&self.pg_pool.clone(), start_block);
            }
            // TODO: update metrics
            // progress_gauge.set(block_number as i64);
        }
        println!("Finished syncing event history");
    }

    pub async fn subscribe_to_latest_events(&self, client: Arc<Provider<Ws>>) {
        let http_client = self.get_client().await;

        let filter = Filter::new().address(self.bridge_address);

        let mut stream = client.subscribe_logs(&filter).await.unwrap();

        println!("Subscribed to latest events");

        while let Some(log) = stream.next().await {
            println!(
                "🔄 Received log at block: {}",
                log.block_number.unwrap().as_u64()
            );

            let log = RawEthLog {
                block_number: log
                    .block_number
                    .ok_or(BridgeError::ProviderError(
                        "Provider returns log without block_number".into(),
                    ))
                    .unwrap()
                    .as_u64(),
                tx_hash: log
                    .transaction_hash
                    .ok_or(BridgeError::ProviderError(
                        "Provider returns log without transaction_hash".into(),
                    ))
                    .unwrap(),
                log,
            };
            // TODO: handle error
            let _ = retry_with_max_elapsed_time!(
                process_eth_events(
                    self.provider.clone(),
                    http_client.clone(),
                    self.pg_pool.clone(),
                    self.metrics.clone(),
                    vec![log.clone()],
                ),
                Duration::from_millis(30000)
            );
        }
    }

    pub async fn subscribe_to_finalized_events(
        &self,
        client: Arc<EthClient<ethers::providers::Http>>,
    ) {
        let mut last_block_number = client.get_last_finalized_block_id().await.unwrap() - 1;
        let mut interval = time::interval(FINALIZED_BLOCK_QUERY_INTERVAL);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let Ok(Ok(new_value)) = retry_with_max_elapsed_time!(
                client.get_last_finalized_block_id(),
                time::Duration::from_secs(600)
            ) else {
                error!("Failed to get last finalized block from eth client after retry");
                continue;
            };
            tracing::debug!("Last finalized block: {}", new_value);

            // TODO add a metrics for the last finalized block

            if new_value > last_block_number {
                // TODO: check for logs from last_block_number to new_value

                let events = retry_with_max_elapsed_time!(
                    client.get_raw_events_in_range(
                        self.bridge_address,
                        last_block_number,
                        new_value
                    ),
                    Duration::from_secs(30000)
                );

                if let Ok(Ok(events)) = events {
                    let events_count = events.len();
                    if events_count > 0 {
                        println!(
                            "⏳ Processing {} events in range {} to {}",
                            events_count, last_block_number, new_value
                        );
                        // TODO: handle error
                        let _ = retry_with_max_elapsed_time!(
                            process_eth_events(
                                self.provider.clone(),
                                client.clone(),
                                self.pg_pool.clone(),
                                self.metrics.clone(),
                                events.clone(),
                            ),
                            Duration::from_millis(30000)
                        );
                        println!(
                            "✅ Processed {} events in range {} to {}",
                            events_count, last_block_number, new_value
                        );
                    }
                }

                last_block_number = new_value;
            }
        }
    }

    async fn get_client(&self) -> Arc<EthClient<ethers::providers::Http>> {
        Arc::new(
            EthClient::<ethers::providers::Http>::new(
                &self.config.eth_rpc_url,
                HashSet::from_iter(vec![self.bridge_address]),
                self.bridge_metrics.clone(),
            )
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))
            .unwrap(),
        )
    }
}

async fn process_eth_events<E: EthEvent>(
    provider: Arc<Provider<Http>>,
    client: Arc<EthClient<ethers::providers::Http>>,
    pg_pool: PgPool,
    metrics: BridgeIndexerMetrics,
    events: Vec<E>,
) -> Result<()> {
    let last_finalized_block = client.get_last_finalized_block_id().await.unwrap();
    let mut transfers = Vec::new();
    let mut block_timestamps: HashMap<u64, u64> = HashMap::new();

    for event in events.iter() {
        let eth_bridge_event = EthBridgeEvent::try_from_log(event.log());
        if eth_bridge_event.is_none() {
            continue;
        }
        metrics.total_eth_bridge_transactions.inc();
        let bridge_event = eth_bridge_event.unwrap();
        let block_number = event.block_number();
        let finalized = block_number <= last_finalized_block;

        // Check if the block timestamp is already in the cache
        let timestamp = if let Some(&cached_timestamp) = block_timestamps.get(&block_number) {
            cached_timestamp
        } else {
            // TODO: handle error
            let block = retry_with_max_elapsed_time!(
                get_block(block_number, &provider),
                Duration::from_millis(30000)
            )
            .unwrap()
            .unwrap();
            let timestamp = block.timestamp.as_u64() * 1000;
            block_timestamps.insert(block_number, timestamp);
            timestamp
        };

        let tx_hash = event.tx_hash();
        // TODO: handle error
        let transaction = retry_with_max_elapsed_time!(
            get_transaction(tx_hash, &provider),
            Duration::from_millis(30000)
        )
        .unwrap()
        .unwrap();
        let gas = transaction.gas;

        let bridge_transfers: Vec<ProcessedTxnData> = match bridge_event {
            EthBridgeEvent::EthSuiBridgeEvents(bridge_event) => match bridge_event {
                EthSuiBridgeEvents::TokensDepositedFilter(bridge_event) => {
                    metrics.total_eth_token_deposited.inc();
                    let transfer = TokenTransfer {
                        chain_id: bridge_event.source_chain_id,
                        nonce: bridge_event.nonce,
                        block_height: block_number,
                        timestamp_ms: timestamp,
                        txn_hash: tx_hash.as_bytes().to_vec(),
                        txn_sender: bridge_event.sender_address.as_bytes().to_vec(),
                        status: TokenTransferStatus::Deposited,
                        gas_usage: gas.as_u64() as i64,
                        data_source: BridgeDataSource::Eth,
                        data: Some(TokenTransferData {
                            sender_address: bridge_event.sender_address.as_bytes().to_vec(),
                            destination_chain: bridge_event.destination_chain_id,
                            recipient_address: bridge_event.recipient_address.to_vec(),
                            token_id: bridge_event.token_id,
                            amount: bridge_event.sui_adjusted_amount,
                        }),
                    };

                    if finalized {
                        vec![
                            ProcessedTxnData::TokenTransfer(transfer.clone()),
                            ProcessedTxnData::TokenTransfer(TokenTransfer {
                                status: TokenTransferStatus::DepositedUnfinalized,
                                ..transfer
                            }),
                        ]
                    } else {
                        vec![ProcessedTxnData::TokenTransfer(transfer)]
                    }
                }
                EthSuiBridgeEvents::TokensClaimedFilter(bridge_event) => {
                    metrics.total_eth_token_transfer_claimed.inc();
                    vec![ProcessedTxnData::TokenTransfer(TokenTransfer {
                        chain_id: bridge_event.source_chain_id,
                        nonce: bridge_event.nonce,
                        block_height: block_number,
                        timestamp_ms: timestamp,
                        txn_hash: tx_hash.as_bytes().to_vec(),
                        txn_sender: bridge_event.sender_address.to_vec(),
                        status: TokenTransferStatus::Claimed,
                        gas_usage: gas.as_u64() as i64,
                        data_source: BridgeDataSource::Eth,
                        data: None,
                    })]
                }
                EthSuiBridgeEvents::PausedFilter(_)
                | EthSuiBridgeEvents::UnpausedFilter(_)
                | EthSuiBridgeEvents::UpgradedFilter(_)
                | EthSuiBridgeEvents::InitializedFilter(_) => {
                    metrics.total_eth_bridge_txn_other.inc();
                    vec![]
                }
            },
            EthBridgeEvent::EthBridgeCommitteeEvents(_)
            | EthBridgeEvent::EthBridgeLimiterEvents(_)
            | EthBridgeEvent::EthBridgeConfigEvents(_)
            | EthBridgeEvent::EthCommitteeUpgradeableContractEvents(_) => {
                metrics.total_eth_bridge_txn_other.inc();
                vec![]
            }
        };

        transfers.extend(bridge_transfers);

        // Batch write all transfers
        if let Err(e) = write(&pg_pool, transfers.clone()) {
            error!("Error writing token transfers to database: {:?}", e);
        }
    }

    Ok(())
}

async fn get_block(block_number: u64, provider: &Arc<Provider<Http>>) -> Result<Block<H256>> {
    Ok(provider.get_block(block_number).await.unwrap().unwrap())
}

async fn get_transaction(tx_hash: H256, provider: &Arc<Provider<Http>>) -> Result<Transaction> {
    Ok(provider.get_transaction(tx_hash).await.unwrap().unwrap())
}
