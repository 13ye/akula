#![feature(never_type)]
#![feature(byte_slice_trim_ascii)]

use min_max::{max, min};
use akula::{
    clkhs::{
        model::{
            TxMessage,
            TxReceiptLog
        },
        execution::{
            clkhs_select_id,
            clkhs_insert_txmsgs,
            clkhs_insert_receipts
        }
    },
    binutil::AkulaDataDir,
    consensus::{engine_factory, Consensus, ForkChoiceMode},
    hex_to_bytes,
    execution,
    kv::{
        tables::{self, BitmapKey, CHAINDATA_TABLES, LOGDATA_TABLES},
        traits::*,
    },
    models::*,
    p2p::node::NodeBuilder,
    stagedsync,
    stages::*, accessors::chain, Buffer, StageId, execution::{analysis_cache::AnalysisCache, tracer::NoopTracer, processor::ExecutionProcessor},
};
use anyhow::{ensure, format_err, Context};
use bytes::Bytes;
use clap::Parser;
use expanded_pathbuf::ExpandedPathBuf;
use jsonrpsee::{core::client::ClientT, http_client::HttpClientBuilder, rpc_params};
use std::{borrow::Cow, collections::BTreeMap, sync::Arc};
use tokio::pin;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};
use url::Url;

#[derive(Parser)]
#[clap(name = "Akula Toolbox", about = "Utilities for Akula Ethereum client")]
struct Opt {
    #[clap(long = "datadir", help = "Database directory path", default_value_t)]
    pub data_dir: AkulaDataDir,

    #[clap(subcommand)]
    pub command: OptCommand,
}

#[derive(Parser)]
pub enum OptCommand {
    /// Print database statistics
    DbStats {
        /// Whether to print CSV
        #[clap(long)]
        csv: bool,
    },

    /// Query database
    DbQuery {
        #[clap(long)]
        table: String,
        #[clap(long, value_parser(hex_to_bytes))]
        key: Bytes,
    },

    /// Walk over table entries
    DbWalk {
        #[clap(long)]
        table: String,
        #[clap(long, value_parser(hex_to_bytes))]
        starting_key: Option<Bytes>,
        #[clap(long)]
        max_entries: Option<usize>,
    },

    /// Set db value
    DbSet {
        #[clap(long)]
        table: String,
        #[clap(long, value_parser(hex_to_bytes))]
        key: Bytes,
        #[clap(long, value_parser(hex_to_bytes))]
        value: Bytes,
    },

    /// Unset db value
    DbUnset {
        #[clap(long)]
        table: String,
        #[clap(long, value_parser(hex_to_bytes))]
        key: Bytes,
    },

    /// Drop db
    DbDrop {
        #[clap(long)]
        table: String,
    },

    /// Save receipt
    DbReceipt {
        #[clap(long)]
        max_height: Option<u64>,
    },

    /// Save txs message
    DbTxslog {
        #[clap(long)]
        max_height: Option<u64>,
        #[clap(long)]
        start_height: Option<u64>,
    },

    /// Check table equality in two databases
    CheckEqual {
        #[clap(long)]
        db1: ExpandedPathBuf,
        #[clap(long)]
        db2: ExpandedPathBuf,
        #[clap(long)]
        table: String,
    },

    /// Execute Block Hashes stage
    Blockhashes,

    /// Execute HeaderDownload stage
    #[clap(name = "download-headers", about = "Run block headers downloader")]
    HeaderDownload {
        #[clap(
            long = "chain",
            help = "Name of the testnet to join",
            default_value = "mainnet"
        )]
        chain: String,

        #[clap(
            long = "sentry.api.addr",
            help = "Sentry GRPC service URL as 'http://host:port'",
            default_value = "http://localhost:8000"
        )]
        uri: tonic::transport::Uri,
    },

    ReadBlock {
        block_number: BlockNumber,
    },

    ReadAccount {
        address: Address,
        block_number: Option<BlockNumber>,
    },

    ReadAccountChanges {
        block: BlockNumber,
    },

    ReadAccountChangedBlocks {
        address: Address,
    },

    ReadStorage {
        address: Address,
    },

    ReadStorageChanges {
        block: BlockNumber,
    },

    /// Overwrite chainspec in database with user-provided one
    OverwriteChainspec {
        chainspec_file: ExpandedPathBuf,
    },

    SendChainTip {
        #[clap(long, default_value = "http://127.0.0.1:8551")]
        endpoint: Url,
        #[clap(long)]
        head: H256,
        #[clap(long)]
        finalized: Option<H256>,
    },

    SetStageProgress {
        #[clap(long)]
        stage: String,
        #[clap(long)]
        progress: BlockNumber,
    },

    UnsetStageProgress {
        #[clap(long)]
        stage: String,
    },
}

async fn download_headers(
    data_dir: AkulaDataDir,
    chain: String,
    uri: tonic::transport::Uri,
) -> anyhow::Result<()> {
    let chain_config = ChainConfig::new(chain.as_ref())?;

    let chain_data_dir = data_dir.chain_data_dir();
    let etl_temp_path = data_dir.etl_temp_dir();

    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let env = Arc::new(akula::kv::new_database(&CHAINDATA_TABLES, &chain_data_dir)?);
    let consensus: Arc<dyn Consensus> =
        engine_factory(Some(env.clone()), chain_config.chain_spec.clone(), None)?.into();
    let txn = env.begin_mutable()?;
    akula::genesis::initialize_genesis(
        &txn,
        &Arc::new(tempfile::tempdir_in(etl_temp_path).context("failed to create ETL temp dir")?),
        true,
        Some(chain_config.chain_spec.clone()),
    )?;

    txn.commit()?;

    let node = Arc::new(
        NodeBuilder::new(chain_config)
            .set_stash(env.clone())
            .add_sentry(uri)
            .build()?,
    );
    tokio::spawn({
        let node = node.clone();
        let tip_discovery = !matches!(consensus.fork_choice_mode(), ForkChoiceMode::External(_));
        async move {
            node.start_sync(tip_discovery).await.unwrap();
        }
    });

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(
        HeaderDownload {
            node,
            consensus,
            max_block: u64::MAX.into(),
            increment: None,
        },
        false,
    );
    staged_sync.run(&env).await?;

    Ok(())
}

async fn blockhashes(data_dir: AkulaDataDir) -> anyhow::Result<()> {
    std::fs::create_dir_all(&data_dir.0)?;

    let etl_temp_path = data_dir.etl_temp_dir();
    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let etl_temp_dir =
        Arc::new(tempfile::tempdir_in(&etl_temp_path).context("failed to create ETL temp dir")?);

    let env = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        &akula::kv::tables::CHAINDATA_TABLES,
    )?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(
        BlockHashes {
            temp_dir: etl_temp_dir.clone(),
        },
        false,
    );
    staged_sync.run(&env).await?;
    Ok(())
}
fn open_db(
    data_dir: AkulaDataDir,
) -> anyhow::Result<akula::kv::mdbx::MdbxEnvironment<mdbx::NoWriteMap>> {
    akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        &CHAINDATA_TABLES,
    )
}

fn open_db_rw(
    data_dir: AkulaDataDir,
) -> anyhow::Result<akula::kv::mdbx::MdbxEnvironment<mdbx::NoWriteMap>> {
    akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        &CHAINDATA_TABLES,
    )
}

fn open_logs_rw(
    data_dir: AkulaDataDir,
) -> anyhow::Result<akula::kv::mdbx::MdbxEnvironment<mdbx::NoWriteMap>> {
    let log_data_dir = data_dir.log_data_dir();
    if !log_data_dir.exists() {
        akula::kv::new_database(&LOGDATA_TABLES, &log_data_dir)?;
    }
    akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &log_data_dir,
        &LOGDATA_TABLES,
    )
}

fn table_sizes(data_dir: AkulaDataDir, csv: bool) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let mut sizes = env.begin()?.table_sizes()?.into_iter().collect::<Vec<_>>();
    sizes.sort_by_key(|(_, size)| *size);

    let mut out = Vec::new();
    if csv {
        out.push("Table,Size".to_string());
        for (table, size) in &sizes {
            out.push(format!("{},{}", table, size));
        }
    } else {
        for (table, size) in &sizes {
            out.push(format!("{} - {}", table, bytesize::ByteSize::b(*size)));
        }
        out.push(format!(
            "TOTAL: {}",
            bytesize::ByteSize::b(sizes.into_iter().map(|(_, size)| size).sum())
        ));
    }

    for line in out {
        println!("{}", line);
    }
    Ok(())
}

fn decode_db_inner<T: Table, K: TableDecode>(
    _: T,
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<(K, T::Value)> {
    Ok((
        <K as TableDecode>::decode(key)?,
        <T::Value as TableDecode>::decode(value)?,
    ))
}

fn decode_db<T: Table>(table: T, key: &[u8], value: &[u8]) -> anyhow::Result<(T::Key, T::Value)>
where
    T::Key: TableDecode,
{
    decode_db_inner::<T, T::Key>(table, key, value)
}

macro_rules! select_db_from_str {
    ($name:expr, [$($table:ident),* $(,)?], $fn:expr, $ow:expr) => {
        match $name {
            $(
                stringify!($table) => $fn($table),
            )*
            _ => $ow,
        }
    };
}

fn select_db_decode(table: &str, key: &[u8], value: &[u8]) -> anyhow::Result<(String, String)> {
    use akula::kv::tables::*;
    select_db_from_str!(
        table,
        [
            Account,
            Storage,
            AccountChangeSet,
            StorageChangeSet,
            HashedAccount,
            HashedStorage,
            AccountHistory,
            StorageHistory,
            Code,
            TrieAccount,
            TrieStorage,
            HeaderNumber,
            CanonicalHeader,
            Header,
            HeadersTotalDifficulty,
            BlockBody,
            BlockTransaction,
            BlockReceipt,
            TotalGas,
            TotalTx,
            LogAddressIndex,
            LogAddressesByBlock,
            LogTopicIndex,
            LogTopicsByBlock,
            CallTraceSet,
            CallFromIndex,
            CallToIndex,
            BlockTransactionLookup,
            Config,
            TxSender,
            Issuance,
            Version,
        ],
        |table| decode_db(table, key, value).map(|(k, v)| (format!("{:?}", k), format!("{:?}", v))),
        select_db_from_str!(
            table,
            [SyncStage],
            |table| decode_db_inner::<_, Vec<u8>>(table, key, value)
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), format!("{:?}", v))),
            anyhow::bail!("unknown table format {}", table)
        )
    )
}

fn db_query(data_dir: AkulaDataDir, table: String, key: Bytes) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let value = txn.get::<Vec<u8>>(&db, &key)?;

    println!("key:     {}", hex::encode(&key));
    println!("value:   {:?}", value.as_ref().map(hex::encode));
    if let Some(value) = &value {
        if let Ok((k, v)) = select_db_decode(&table, &key, value) {
            println!("decoded: {} => {}", k, v);
        }
    }

    Ok(())
}

fn db_walk(
    data_dir: AkulaDataDir,
    table: String,
    starting_key: Option<Bytes>,
    max_entries: Option<usize>,
) -> anyhow::Result<()> {
    let env = if table != "BlockReceipt" {
        open_db(data_dir)?
    } else {
        open_logs_rw(data_dir)?
    };

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let mut cur = txn.cursor(&db)?;
    for (i, item) in if let Some(starting_key) = starting_key {
        cur.iter_from::<Cow<[u8]>, Cow<[u8]>>(&starting_key)
    } else {
        cur.iter::<Cow<[u8]>, Cow<[u8]>>()
    }
    .enumerate()
    .take(max_entries.unwrap_or(usize::MAX))
    {
        let (k, v) = item?;
        println!(
            "{} / {:?} / {:?} / {:?}",
            i,
            hex::encode(&k),
            hex::encode(&v),
            select_db_decode(&table, &k, &v),
        );
    }

    Ok(())
}

fn db_set(
    data_dir: AkulaDataDir,
    table: String,
    key: Bytes,
    value: Option<Bytes>,
) -> anyhow::Result<()> {
    let env = open_db_rw(data_dir)?;

    let txn = env.begin_rw_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    if let Some(value) = value {
        txn.put(&db, key, value, Default::default())?;
    } else {
        txn.del(&db, key, None)?;
    }
    txn.commit()?;

    Ok(())
}

fn db_drop(data_dir: AkulaDataDir, table: String) -> anyhow::Result<()> {
    let env = open_db_rw(data_dir)?;

    let txn = env.begin_rw_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    unsafe {
        txn.drop_db(db)?;
    }
    txn.commit()?;

    Ok(())
}

fn db_receipt(data_dir: AkulaDataDir, max_height: Option<u64>) -> anyhow::Result<()> {
    let env = open_db(data_dir.clone())?;
    let log_env = open_logs_rw(data_dir)?;

    let txn = env.begin()?;
    let log_txn = log_env.begin_mutable()?;

    const STAGE_ID: StageId = StageId("Receipt");

    let chain_spec = chain::chain_config::read(&txn)?
        .ok_or_else(|| format_err!("chain specification not found"))?;
    let mut block_number = STAGE_ID.get_progress(&log_txn)?.unwrap_or_default();
    let tip = EXECUTION.get_progress(&txn)?.unwrap_or_default();

    // Prepare the execution context.

    let mut engine = engine_factory(None, chain_spec.clone(), None)?;
    let mut analysis_cache = AnalysisCache::default();
    let mut tracer = NoopTracer;

    while block_number < tip {
        if let Some(max_height) = max_height {
            if max_height <= block_number.0 {
                break;
            }
        }
        let mut buffer = Buffer::new(&txn, Some(block_number));

        let start_idx = txn.get(tables::TotalTx, block_number)?.ok_or_else(|| {
                format_err!("totaltx not calculated for block #{block_number}")
            })?;
        block_number.0 += 1;
        if block_number.0 % 100_000 == 0 {
            println!("running block #{block_number}");
        }
        let header = chain::header::read(&txn, block_number)?.ok_or_else(|| {
                format_err!("header not found for block #{block_number}")
            })?;
        let block_body = chain::block_body::read_with_senders(&txn, block_number)?
            .ok_or_else(|| {
                format_err!("body not found for block #{block_number}")
            })?;
        let block_execution_spec = chain_spec.collect_block_spec(block_number);

        let mut processor = ExecutionProcessor::new(
            &mut buffer,
            &mut tracer,
            &mut analysis_cache,
            &mut *engine,
            &header,
            &block_body,
            &block_execution_spec,
        );

        let receipts = processor.execute_block_no_post_validation()?;
        for (idx, receipt) in receipts.into_iter().enumerate() {
            // if !receipt.logs.is_empty() {
            //     let tx = chain::tx::read(&txn, idx as u64 + start_idx, 1)?;
            //     println!("{}:{}+{} {} => {:?} {:?}", block_number, start_idx, idx, receipt.logs.len(), tx[0].hash(), tx[0].message);
            // }
            log_txn.set(tables::BlockReceipt, TxIndex(idx as u64 + start_idx), receipt)?;
        }
    }
    STAGE_ID.save_progress(&log_txn, block_number)?;

    log_txn.commit()?;

    Ok(())
}

fn db_txs_logs(data_dir: AkulaDataDir, max_height: Option<u64>, start_height: Option<u64>, start_logs_id:u64) -> anyhow::Result<(Vec<TxReceiptLog>, Vec<TxMessage>)> {
    let env = open_db(data_dir.clone())?;
    let txn = env.begin()?;

    let mut block_number = BlockNumber(0);
    if let Some(start_height) = start_height {
        block_number = BlockNumber(start_height);
    }
    let chaintip = EXECUTION.get_progress(&txn)?.unwrap_or_default();

    // already query next logs_id
    let mut logs_id:u64 =  start_logs_id;
    let mut tx_idx = TxIndex(0);

    // Prepare the execution context.
    let chain_spec = chain::chain_config::read(&txn)?
        .ok_or_else(|| format_err!("chain specification not found"))?;
    let mut engine = engine_factory(None, chain_spec.clone(), None)?;
    let mut analysis_cache = AnalysisCache::default();
    let mut tracer = NoopTracer;

    // Prepare MsgSignature DB
    let table_msg = "BlockTransaction";
    let txn_ro= env.begin_ro_txn()?;
    let db_msg = txn_ro
        .open_db(Some(&table_msg))
        .with_context(|| format!("failed to open table: {}", table_msg))?;

    // Prepare TxSender DB
    let table_sender = "TxSender";
    let db_sender = txn_ro
        .open_db(Some(&table_sender))
        .with_context(|| format!("failed to open table: {}", table_sender))?;

    let mut vec_logs:Vec<TxReceiptLog> = Vec::new();
    let mut vec_txmsg:Vec<TxMessage> = Vec::new();
    println!("start_block:{block_number} block_sync:{chaintip} max_height:{:?}", max_height);
    let block_stat_time = std::time::Instant::now();
    let idx_stat_time = std::time::Instant::now();
    while block_number < chaintip {
        if let Some(max_height) = max_height {
            if max_height <= block_number.0 {
                break;
            }
        }
        if block_number.0 % 10_000 == 0 {
            println!("running block #{block_number} time_elapsed[{:?}]", block_stat_time.elapsed());
        }
        let mut buffer = Buffer::new(&txn, Some(block_number));
        let start_idx = txn.get(tables::TotalTx, block_number)?.ok_or_else(|| {
                format_err!("totaltx not calculated for block #{block_number}")
            })?;
        block_number.0 += 1;
        let end_idx = txn.get(tables::TotalTx, block_number)?.ok_or_else(|| {
                format_err!("totaltx not calculated for block #{block_number}")
            })?;
        let header = chain::header::read(&txn, block_number)?.ok_or_else(|| {
                format_err!("header not found for block #{block_number}")
            })?;
        let block_body = chain::block_body::read_with_senders(&txn, block_number)?
            .ok_or_else(|| {
                format_err!("body not found for block #{block_number}")
            })?;
        let block_execution_spec = chain_spec.collect_block_spec(block_number);

        //println!("blok_num[{block_number}] spec_{:#?}\n& header[{:#?}]\n&block_bod[{:#?}]", block_execution_spec, header, block_body);

        let mut processor = ExecutionProcessor::new(
            &mut buffer,
            &mut tracer,
            &mut analysis_cache,
            &mut *engine,
            &header,
            &block_body,
            &block_execution_spec,
        );

        // get Receipts in this block
        let receipts = processor.execute_block_no_post_validation()?;

        // get TxSenders in this block
        let key_blk_number = u64::from(block_number).to_be_bytes();
        let tx_senders_op = txn_ro.get::<Vec<u8>>(&db_sender, &key_blk_number)?;
        let mut tx_senders:Vec<Address> = Vec::new();
        if let Some(tx_senders_op) = tx_senders_op{
            for i in 0..tx_senders_op.len()/20{
                let mut addr_bytes = [0u8; 20];
                addr_bytes.copy_from_slice(&tx_senders_op[i*20..(i+1)*20]);
                tx_senders.push(Address::from(addr_bytes));
            }
        }

        // start iterate TxMessage in this block, also Receipts, and push them into vecs
        let mut cumulative_gas_used = 0;
        for idx in start_idx..end_idx{
            if idx % 1_000 == 0{
                println!("block_num[{block_number}] txs_cnt[{idx}] logs_id[{logs_id}] tx_senders[{:?}] len_vec_logs[{:?}] len_vec_txmsg[{:?}] idx_time_elapsed[{:?}]", tx_senders.len(), vec_logs.len(), vec_txmsg.len(), idx_stat_time.elapsed());
            }
            tx_idx = TxIndex(idx as u64);
            let key_idx = u64::from(tx_idx).to_be_bytes();
            let msg_bytes = txn_ro.get::<Vec<u8>>(&db_msg, &key_idx)?;

            if let Some(msg_bytes) = &msg_bytes{
                match MessageWithSignature::compact_decode(msg_bytes){
                    Err(e) => {
                        println!("Err in decoding MessageWithSignature, {:?}",e);
                        return Err(e);
                    },
                    Ok(message_with_signature) => {
                        let receipt = receipts[(idx-start_idx) as usize].clone();
                        let input_len = message_with_signature.message.input().len();

                        let mut inputs_4bytes = [0u8; 4];
                        if input_len > 0{
                            inputs_4bytes[..min(4, input_len)].copy_from_slice(message_with_signature.message.input().slice(0..min(input_len, 4)).as_ref());
                        }

                        let mut row_txs : TxMessage = TxMessage {
                            idx,
                            idx_in_block: idx-start_idx,
                            block_number: block_number.0,
                            hash: primitive_types::U256::from_big_endian(message_with_signature.hash().as_bytes()),
                            chain_id: None,
                            nonce: message_with_signature.message.nonce(),
                            gas_limit: message_with_signature.message.gas_limit(),
                            gas_used: receipt.cumulative_gas_used - cumulative_gas_used, // receipt
                            gas_priority_fee: primitive_types::U256::from_big_endian(message_with_signature.message.max_priority_fee_per_gas().to_be_bytes().as_slice()),
                            gas_fee: primitive_types::U256::from_big_endian(message_with_signature.message.max_fee_per_gas2().to_be_bytes().as_slice()),
                            transfer: primitive_types::U256::from_big_endian(message_with_signature.message.value().to_be_bytes().as_slice()),
                            input_len: input_len as u64,
                            input_first_4bytes: u32::from_be_bytes(inputs_4bytes),//if input_len==0 {0} else {u32::from_be_bytes(message_with_signature.message.input().slice(0..min(input_len, 4)).try_into().unwrap())},
                            input_last_32bytes: if input_len==0 {primitive_types::U256::from(0x0)} else {primitive_types::U256::from_big_endian(message_with_signature.message.input().slice(max(0, input_len as i64 - 32) as usize..).as_ref().try_into().unwrap())},
                            from: if (tx_senders.len() <= (idx-start_idx) as usize) {primitive_types::U256::from(0x0)} else{primitive_types::U256::from_big_endian(tx_senders[(idx-start_idx) as usize].as_bytes())}, // txsender
                            to: primitive_types::U256::from(0),
                            success: receipt.success, // receipt
                            is_create: false,
                            logs_count: receipt.logs.len() as u64, // receipt
                        };
                        // chain_id may by None
                        if let Some(chain_id) = message_with_signature.message.chain_id(){
                            //row_txs.chain_id = chain_id.as_i256().0[0];
                            row_txs.chain_id = Some(u64::from_be_bytes(chain_id.to_be_bytes()));
                        }
                        // Action call, recording address as 'to', else record as contract_address... both record into field 'to'
                        if message_with_signature.message.action() == TransactionAction::Create{
                            row_txs.is_create = true;
                            if (tx_senders.len() > (idx-start_idx) as usize) {
                               row_txs.to = primitive_types::U256::from_big_endian(execution::address::create_address(tx_senders[(idx-start_idx) as usize], message_with_signature.message.nonce()).as_bytes());
                            }
                        }
                        else{
                            let call_address = message_with_signature.message.action().into_address();
                            if let Some(call_address) = call_address{
                                row_txs.to = primitive_types::U256::from_big_endian(call_address.as_bytes());
                            }
                        }

                        for (_id_log, log_obj) in receipt.logs.into_iter().enumerate(){
                            logs_id += 1;
                            let data_len = log_obj.data.len();
                            let row_log : TxReceiptLog = TxReceiptLog {
                                id: logs_id,
                                tx_idx: u64::from(tx_idx),
                                tx_message_hash: row_txs.hash,
                                address: primitive_types::U256::from_big_endian(log_obj.address.as_bytes()),
                                data_len: data_len as u64,
                                data_prefix32: if data_len==0 {primitive_types::U256::from(0x0)} else {primitive_types::U256::from_big_endian(log_obj.data.slice(0..min!(32, data_len)).as_ref().try_into().unwrap())},
                                data_prefix128: if data_len==0 {Vec::new()} else {log_obj.data.slice(0..min!(128, data_len)).to_vec()},
                                topic_num: log_obj.topics.len() as u8,
                                topic0: if log_obj.topics.len()<1 {primitive_types::U256::from(0x0)} else {primitive_types::U256::from_big_endian(log_obj.topics[0].as_bytes())},
                                topic1: if log_obj.topics.len()<2 {primitive_types::U256::from(0x0)} else {primitive_types::U256::from_big_endian(log_obj.topics[1].as_bytes())},
                                topic2: if log_obj.topics.len()<3 {primitive_types::U256::from(0x0)} else {primitive_types::U256::from_big_endian(log_obj.topics[2].as_bytes())},
                                topic3: if log_obj.topics.len()<4 {primitive_types::U256::from(0x0)} else {primitive_types::U256::from_big_endian(log_obj.topics[3].as_bytes())},
                                topic4: if log_obj.topics.len()<5 {primitive_types::U256::from(0x0)} else {primitive_types::U256::from_big_endian(log_obj.topics[4].as_bytes())}
                            };
                            vec_logs.push(row_log);
                        } // end of for_logs

                        // finish txmessage construct, push into vec
                        vec_txmsg.push(row_txs);
                        cumulative_gas_used = receipt.cumulative_gas_used;
                    }
                }
            }
        } // end of for_idx
    } // end of while_block_number

    println!("Finish processing data, ready to insert into database, blockNumber[{:?}] tx_idx[{:?}] logs_id[{logs_id}] len_log[{:?}] len_txmsg[{:?}]", block_number, tx_idx, vec_logs.len(), vec_txmsg.len());
    Ok((vec_logs, vec_txmsg))
}


fn check_table_eq(
    db1_path: ExpandedPathBuf,
    db2_path: ExpandedPathBuf,
    table: String,
) -> anyhow::Result<()> {
    let env1 = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db1_path,
        &CHAINDATA_TABLES,
    )?;
    let env2 = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db2_path,
        &CHAINDATA_TABLES,
    )?;

    let txn1 = env1.begin_ro_txn()?;
    let txn2 = env2.begin_ro_txn()?;
    let db1 = txn1
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let db2 = txn2
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let mut cur1 = txn1.cursor(&db1)?;
    let mut cur2 = txn2.cursor(&db2)?;

    let mut kv1 = cur1.next::<Cow<[u8]>, Cow<[u8]>>()?;
    let mut kv2 = cur2.next::<Cow<[u8]>, Cow<[u8]>>()?;
    loop {
        match (&kv1, &kv2) {
            (None, None) => break,
            (None, Some((k2, v2))) => {
                info!("Missing in db1: {} [{}]", hex::encode(k2), hex::encode(v2));

                kv2 = cur2.next()?;
            }
            (Some((k1, v1)), None) => {
                info!("Missing in db2: {} [{}]", hex::encode(k1), hex::encode(v1));

                kv1 = cur1.next()?;
            }
            (Some((k1, v1)), Some((k2, v2))) => match k1.cmp(k2) {
                std::cmp::Ordering::Greater => {
                    info!("Missing in db1: {} [{}]", hex::encode(k2), hex::encode(v2));

                    kv2 = cur2.next()?;
                }
                std::cmp::Ordering::Less => {
                    info!("Missing in db2: {} [{}]", hex::encode(k1), hex::encode(v1));

                    kv1 = cur1.next()?;
                }
                std::cmp::Ordering::Equal => {
                    if v1 != v2 {
                        info!(
                            "Mismatch for key: {} [{} != {}]",
                            hex::encode(k1),
                            hex::encode(v1),
                            hex::encode(v2)
                        );
                    }

                    kv1 = cur1.next()?;
                    kv2 = cur2.next()?;
                }
            },
        }
    }

    Ok(())
}

fn read_block(data_dir: AkulaDataDir, block_num: BlockNumber) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let header = akula::accessors::chain::header::read(&tx, block_num)?
        .ok_or_else(|| format_err!("header not found"))?;
    let body = akula::accessors::chain::block_body::read_without_senders(&tx, block_num)?
        .ok_or_else(|| format_err!("block body not found"))?;

    let partial_header = PartialHeader::from(header.clone());

    let block = Block::new(partial_header.clone(), body.transactions, body.ommers);

    ensure!(
        block.header.transactions_root == header.transactions_root,
        "root mismatch: expected in header {:?}, computed {:?}",
        header.transactions_root,
        block.header.transactions_root
    );
    ensure!(
        block.header.ommers_hash == header.ommers_hash,
        "root mismatch: expected in header {:?}, computed {:?}",
        header.ommers_hash,
        block.header.ommers_hash
    );

    println!("{:?}", partial_header);
    println!("OMMERS:");
    for (i, v) in block.ommers.into_iter().enumerate() {
        println!("[{}] {:?}", i, v);
    }

    println!("TRANSACTIONS:");
    for (i, v) in block.transactions.into_iter().enumerate() {
        println!("[{}/{:?}] {:?}", i, v.hash(), v);
    }

    Ok(())
}

fn read_account(
    data_dir: AkulaDataDir,
    address: Address,
    block_number: Option<BlockNumber>,
) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let account = akula::accessors::state::account::read(&tx, address, block_number)?;

    println!("{:?}", account);

    Ok(())
}

fn read_account_changes(data_dir: AkulaDataDir, block: BlockNumber) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let walker = tx.cursor(tables::AccountChangeSet)?.walk_dup(block, None);

    pin!(walker);

    while let Some(tables::AccountChange { address, account }) = walker.next().transpose()? {
        println!("{:?}: {:?}", address, account);
    }

    Ok(())
}

fn read_account_changed_blocks(data_dir: AkulaDataDir, address: Address) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let walker = tx.cursor(tables::AccountHistory)?.walk(Some(BitmapKey {
        inner: address,
        block_number: BlockNumber(0),
    }));

    pin!(walker);

    while let Some((key, bitmap)) = walker.next().transpose()? {
        if key.inner != address {
            break;
        }

        println!("up to {}: {:?}", key.block_number, bitmap);
    }

    Ok(())
}

fn read_storage(data_dir: AkulaDataDir, address: Address) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    println!(
        "{:?}",
        tx.cursor(tables::Storage)?
            .walk_dup(address, None)
            .collect::<anyhow::Result<Vec<_>>>()?
    );

    Ok(())
}

fn read_storage_changes(data_dir: AkulaDataDir, block: BlockNumber) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let cur = tx.cursor(tables::StorageChangeSet)?;

    let walker = cur.walk(Some(block));

    pin!(walker);

    let mut changes = BTreeMap::<Address, BTreeMap<H256, U256>>::new();
    while let Some((
        tables::StorageChangeKey {
            block_number,
            address,
        },
        tables::StorageChange { location, value },
    )) = walker.next().transpose()?
    {
        if block_number > block {
            break;
        }

        changes.entry(address).or_default().insert(location, value);
    }

    for (address, slots) in changes {
        println!("{:?}: {:?}", address, slots);
    }

    Ok(())
}

fn overwrite_chainspec(
    data_dir: AkulaDataDir,
    chainspec_file: ExpandedPathBuf,
) -> anyhow::Result<()> {
    let new_chainspec = ChainSpec::load_from_file(chainspec_file)?;

    let chain_data_dir = data_dir.chain_data_dir();

    let env = Arc::new(akula::kv::new_database(&CHAINDATA_TABLES, &chain_data_dir)?);

    let tx = env.begin_mutable()?;

    tx.set(tables::Config, (), new_chainspec)?;
    tx.commit()?;

    Ok(())
}

async fn send_chain_tip(endpoint: Url, head: H256, finalized: Option<H256>) -> anyhow::Result<()> {
    let client = HttpClientBuilder::default().build(endpoint)?;
    let state = ethereum_jsonrpc::ForkchoiceState {
        head_block_hash: head,
        safe_block_hash: H256::default(),
        finalized_block_hash: finalized.unwrap_or_default(),
    };
    println!("{:?}", state);
    let params = rpc_params![state, None::<ethereum_jsonrpc::PayloadAttributes>];
    let result: ethereum_jsonrpc::ForkchoiceUpdatedResponse =
        client.request("engine_forkchoiceUpdatedV1", params).await?;
    println!("{:?}", result);
    Ok(())
}

fn set_stage_progress(
    data_dir: AkulaDataDir,
    stage: String,
    stage_progress: Option<BlockNumber>,
) -> anyhow::Result<()> {
    db_set(
        data_dir,
        tables::SyncStage::const_db_name().to_string(),
        stage.as_bytes().to_vec().into(),
        stage_progress.map(|v| v.encode().to_vec().into()),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::parse();

    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(filter)
        .init();

    match opt.command {
        OptCommand::DbStats { csv } => table_sizes(opt.data_dir, csv)?,
        OptCommand::Blockhashes => blockhashes(opt.data_dir).await?,
        OptCommand::HeaderDownload { chain, uri } => {
            download_headers(opt.data_dir, chain, uri).await?
        }
        OptCommand::DbQuery { table, key } => db_query(opt.data_dir, table, key)?,
        OptCommand::DbWalk {
            table,
            starting_key,
            max_entries,
        } => db_walk(opt.data_dir, table, starting_key, max_entries)?,
        OptCommand::DbSet { table, key, value } => db_set(opt.data_dir, table, key, Some(value))?,
        OptCommand::DbUnset { table, key } => db_set(opt.data_dir, table, key, None)?,
        OptCommand::DbDrop { table } => db_drop(opt.data_dir, table)?,

        OptCommand::DbReceipt { max_height } => {
            std::thread::Builder::new()
                .stack_size(128 * 1024 * 1024)
                .spawn(move || {
                    return db_receipt(opt.data_dir, max_height)
            }).expect("spawn").join().expect("join").ok();
        }

        OptCommand::DbTxslog { max_height, start_height } => {
            let mut start_logs_id:u64 = 0;
            futures::executor::block_on(async{
                let id_rlt = clkhs_select_id().await;
                match id_rlt {
                    Ok(id_rlt)=> {
                        start_logs_id = id_rlt;
                    },
                    Err(error) => {
                        println!("clkhs_select_logs_id_error_occurs{:?}", error);
                    } ,
                };
            });
            println!("@@@ begin processing data, max_h[{:?}] start_h[{:?}] start_log_id[{start_logs_id}]", max_height, start_height);

            let vec_rlts = std::thread::Builder::new()
                .stack_size(128 * 1024 * 1024)
                .spawn(move || {
                    return db_txs_logs(opt.data_dir, max_height, start_height, start_logs_id);
                }).expect("spawn").join().expect("join").ok();

            if let Some(vec_rlts) = vec_rlts{
                let vec_logs = vec_rlts.0;
                let vec_txmsgs = vec_rlts.1;
                println!("@@@ start insert into database! logs_len[{:?}], txmsgs_len[{:?}]", vec_logs.len(), vec_txmsgs.len());

                futures::executor::block_on(async{
                    let rlt = clkhs_insert_txmsgs(&vec_txmsgs).await;
                    match rlt {
                        Ok(())=> { },
                        Err(error) => {
                            panic!("insert_clkhs_txs_error_occurs{:?}", error);
                        } ,
                    };
                });
                println!("@@@ finish insert_txs into database!");
                futures::executor::block_on(async{
                    let rlt = clkhs_insert_receipts(&vec_logs).await;
                    match rlt {
                        Ok(())=> { },
                        Err(error) => {
                            panic!("insert_clkhs_logs_error_occurs{:?}", error);
                        } ,
                    };
                });
                println!("@@@ finish insert_logs into database!");
            }
        }

        OptCommand::CheckEqual { db1, db2, table } => check_table_eq(db1, db2, table)?,
        OptCommand::ReadBlock { block_number } => read_block(opt.data_dir, block_number)?,
        OptCommand::ReadAccount {
            address,
            block_number,
        } => read_account(opt.data_dir, address, block_number)?,
        OptCommand::ReadAccountChanges { block } => read_account_changes(opt.data_dir, block)?,
        OptCommand::ReadAccountChangedBlocks { address } => {
            read_account_changed_blocks(opt.data_dir, address)?
        }
        OptCommand::ReadStorage { address } => read_storage(opt.data_dir, address)?,
        OptCommand::ReadStorageChanges { block } => read_storage_changes(opt.data_dir, block)?,
        OptCommand::OverwriteChainspec { chainspec_file } => {
            overwrite_chainspec(opt.data_dir, chainspec_file)?
        }
        OptCommand::SendChainTip {
            endpoint,
            head,
            finalized,
        } => send_chain_tip(endpoint, head, finalized).await?,
        OptCommand::SetStageProgress { stage, progress } => {
            set_stage_progress(opt.data_dir, stage, Some(progress))?
        }
        OptCommand::UnsetStageProgress { stage } => set_stage_progress(opt.data_dir, stage, None)?,
    }

    Ok(())
}
