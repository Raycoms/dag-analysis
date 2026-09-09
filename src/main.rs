use rocksdb::{Options, DB};
use serde::Deserialize;
use statrs::statistics::{Data, Distribution, Max, OrderStatistics};
use std::collections::{HashSet};
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use async_channel::Sender;
use dashmap::{DashMap};
use roaring::RoaringBitmap;
use tidehunter;
use tidehunter::config::Config;
use tidehunter::db::{Db};
use tidehunter::metrics::Metrics;
use tokio::sync::mpsc;

type Round = u32;
type AuthorityIndex = u32;
type Epoch = u64;
type BlockTimestampMs = u64;
type CommitIndex = u32;
type TransactionIndex = u16;

#[derive(Deserialize, Clone, Copy)]
struct BlockDigest([u8; 32]);

#[derive(Deserialize, Clone, Copy)]
struct BlockRef {
    round: Round,
    author: AuthorityIndex,
    digest: BlockDigest,
}

#[derive(Deserialize)]
struct Transaction {
    data: Vec<u8>,
}

#[derive(Deserialize)]
struct CommitDigest([u8; 32]);

#[derive(Deserialize)]
struct CommitVote {
    index: CommitIndex,
    digest: CommitDigest,
}

#[derive(Deserialize)]
enum MisbehaviorProof {
    InvalidBlock(#[allow(dead_code)] BlockRef),
}

#[derive(Deserialize)]
struct MisbehaviorReport {
    target: AuthorityIndex,
    proof: MisbehaviorProof,
}

#[derive(Deserialize)]
struct BlockTransactionVotes {
    #[allow(dead_code)]
    block_ref: BlockRef,
    #[allow(dead_code)]
    rejects: Vec<TransactionIndex>,
}

#[derive(Deserialize)]
struct BlockV1 {
    epoch: Epoch,
    round: Round,
    author: AuthorityIndex,
    timestamp_ms: BlockTimestampMs,
    ancestors: Vec<BlockRef>,
    transactions: Vec<Transaction>,
    commit_votes: Vec<CommitVote>,
    misbehavior_reports: Vec<MisbehaviorReport>,
}

#[derive(Deserialize)]
struct BlockV2 {
    epoch: Epoch,
    round: Round,
    author: AuthorityIndex,
    #[allow(dead_code)]
    timestamp_ms: BlockTimestampMs,
    ancestors: Vec<BlockRef>,
    transactions: Vec<Transaction>,
    #[allow(dead_code)]
    transaction_votes: Vec<BlockTransactionVotes>,
    #[allow(dead_code)]
    commit_votes: Vec<CommitVote>,
    #[allow(dead_code)]
    misbehavior_reports: Vec<MisbehaviorReport>,
}

#[derive(Deserialize)]
struct BlockV3 {
    epoch: Epoch,
    round: Round,
    author: AuthorityIndex,
    #[allow(dead_code)]
    timestamp_ms: BlockTimestampMs,
    ancestors: Vec<BlockRef>,
    transactions: Vec<Transaction>,
    #[allow(dead_code)]
    transaction_votes: Vec<BlockTransactionVotes>,
    #[allow(dead_code)]
    transaction_votes_cutoff_round: Round,
    #[allow(dead_code)]
    commit_votes: Vec<CommitVote>,
    #[allow(dead_code)]
    misbehavior_reports: Vec<MisbehaviorReport>,
}

#[derive(Deserialize)]
enum Block {
    V1(BlockV1),
    V2(BlockV2),
    V3(BlockV3),
}

#[derive(Deserialize)]
struct SignedBlock {
    inner: Block,
    signature: Vec<u8>,
}

fn parse_block_value(value_bytes: &[u8]) -> bcs::Result<SignedBlock> {
    let inner_bytes: Vec<u8> = bcs::from_bytes(value_bytes)?;
    bcs::from_bytes(&inner_bytes)
}

struct BlockMetrics {
    total_size_kb: f64,
    num_references: u32,
    references_size_kb: f64,
    num_transactions: u32,
    payload_size_kb: f64,
    overlap: i64,
    round: u32,
    per_tx_size: u32,
    bitmap_size: u32,
    act_num_transactions: u32,
}

/// Pulls the fields common to all three Block versions out through a single
/// match, so the metrics logic below doesn't need to be duplicated per
/// version. (epoch/timestamp_ms aren't used in metrics currently but are
/// included for parity with the fields available on every version.)
fn common_fields(
    b: &Block,
) -> (
    Round,
    AuthorityIndex,
    &Vec<BlockRef>,
    &Vec<Transaction>,
    &'static str,
) {
    match b {
        Block::V1(b) => (b.round, b.author, &b.ancestors, &b.transactions, "V1"),
        Block::V2(b) => (b.round, b.author, &b.ancestors, &b.transactions, "V2"),
        Block::V3(b) => (b.round, b.author, &b.ancestors, &b.transactions, "V3"),
    }
}

fn compute_metrics(
    value_bytes: &[u8],
    signed: &SignedBlock,
    author_to_ref_map: &mut Arc<DashMap<AuthorityIndex, HashSet<AuthorityIndex>>>,
    tx_tracker: &mut Arc<DashMap<u32, (u32, Vec<Vec<u8>>)>>,
) -> BlockMetrics {
    let (round, author, ancestors, transactions, version) = common_fields(&signed.inner);

    let total_size = value_bytes.len();
    let num_references = ancestors.len() as u32;
    let reference_byte_size = (num_references as usize) * (32 + 4 + 4); // fixed 40 bytes/ref

    let auths: HashSet<AuthorityIndex> = ancestors.iter().map(|r| r.author).collect();

    let overlap: i64 = if let Some(prev) = author_to_ref_map.get(&author) {
        let missing = prev.difference(&auths).count();
        let extra = auths.difference(&prev).count();
        (missing + extra) as i64
    } else {
        -1
    };

    let mut dup_tx = 0;
    let mut entry = tx_tracker.entry(round % 10).or_insert_with(|| (round, Vec::new()));

    // stale bucket from a previous round that hashed to the same slot
    if entry.0 != round {
        entry.0 = round;
        entry.1.clear(); // reuse allocation instead of a fresh Vec
    }

    //let mut bitmap = RoaringBitmap::default();
    //for author in auths.iter() {
    //    bitmap.insert(*author);
    //}

    author_to_ref_map.insert(author, auths);

    let total_tx_payload_bytes: u64 = transactions.iter().map(|t| t.data.len() as u64).sum();

    for tx in transactions.iter() {
        if entry.1.contains(&tx.data) {
            dup_tx += 1;
        } else {
            entry.1.push(tx.data.clone());
        }
    }

    let mut per_tx_size: u32 = 0;
    if transactions.len() > 0 {
        per_tx_size = (total_tx_payload_bytes / transactions.len() as u64) as u32;
    }

    BlockMetrics {
        total_size_kb: total_size as f64 / 1024.0,
        num_references,
        references_size_kb: reference_byte_size as f64 / 1024.0,
        num_transactions: transactions.len() as u32,
        act_num_transactions: (transactions.len() - dup_tx) as u32,
        payload_size_kb: total_tx_payload_bytes as f64,
        overlap,
        round: round,
        per_tx_size,
        //bitmap_size: bitmap.serialized_size() as u32,
        bitmap_size: 0
    }
}

fn stats_summary(xs: &[f64]) -> (f64, f64, f64, f64, f64, f64, f64) {
    let mut data = Data::new(xs.to_vec());
    let mean = data.mean().unwrap_or(f64::NAN);
    let p1 = data.percentile(1);
    let p10 = data.percentile(10);
    let p50 = data.percentile(50);
    let p90 = data.percentile(90);
    let p99 = data.percentile(99);
    let max = data.max();
    (mean, p1, p10, p50, p90, p99, max)
}

fn load_from_path(db_path: PathBuf, task_sender: Sender<Vec<u8>>) {
    tokio::spawn(async move {
        let mut opts = Options::default();
        opts.create_if_missing(false);

        let rocks_success = match DB::list_cf(&opts, &db_path) {
            Ok(cf_names) => {
                if let Ok(db) = DB::open_cf(&opts, &db_path, &cf_names) {
                    if let Some(cf) = db.cf_handle("blocks") {
                        for item in db.iterator_cf(cf, rocksdb::IteratorMode::Start) {
                            let (_key, value) = match item {
                                Ok(kv) => kv,
                                Err(e) => {
                                    eprintln!("RocksDB iterator error: {}", e);
                                    break;
                                }
                            };
                            let _ = task_sender.send(value.to_vec()).await;
                        }
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Err(_) => false,
        };

        if rocks_success {
            return;
        }

        eprintln!("RocksDB read failed, trying tidehunter...");

        match Db::load_key_shape(&db_path) {
            Ok(key_shape) => {
                match Db::open(&db_path, key_shape, Arc::new(Config::default()), Metrics::new()) {
                    Ok(db) => {
                        if let Some(ks) = db.try_ks("blocks") {
                            let mut iter = db.iterator(ks);

                            while let Some(entry) = iter.next() {
                                match entry {
                                    Ok((key, value)) => {
                                        let _ = task_sender.send(value.to_vec()).await;
                                    }
                                    Err(e) => {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    });
}

#[tokio::main]
async fn main() {
    let db_path = env::home_dir()
        .unwrap()
        .as_path()
        .join("Downloads/opt/sui/db/consensus_db/1230"); // 648 1230

    let max_round: u32 = 1_000_000;
    let limit: usize = max_round as usize * 108;

    let author_to_ref_map: Arc<DashMap<AuthorityIndex, HashSet<AuthorityIndex>>> = Arc::new(DashMap::new());
    let tx_tracker: Arc<DashMap<u32, (u32, Vec<Vec<u8>>)>> = Arc::new(DashMap::new());

    let mut block_size = Vec::with_capacity(limit);
    let mut num_tx = Vec::with_capacity(limit);
    let mut act_num_tx = Vec::with_capacity(limit);

    let mut tx_payload_size_per_block = Vec::with_capacity(limit);
    let mut num_references = Vec::with_capacity(limit);
    let mut reference_size = Vec::with_capacity(limit);
    let mut reference_pct = Vec::with_capacity(limit);
    let mut overlap = Vec::with_capacity(limit);
    let mut tx_size = Vec::with_capacity(limit);
    let mut bitmap_size = Vec::with_capacity(limit);

    let start_time = Instant::now();

    let (task_sender, task_receiver) = async_channel::bounded(1000);
    load_from_path(db_path, task_sender);

    let (block_sender, mut block_receiver) = mpsc::channel(1000);
    for _ in 0..10 {
        let block_sender = block_sender.clone();
        let task_receiver = task_receiver.clone();
        let mut author_to_ref_map = author_to_ref_map.clone();
        let mut tx_tracker = tx_tracker.clone();

        tokio::spawn(async move {
            while let Ok(value) = task_receiver.recv().await {
                match parse_block_value(&value) {
                    Ok(signed) => {
                        let _ = block_sender.send(compute_metrics(&value, &signed, &mut author_to_ref_map, &mut tx_tracker)).await;
                    }
                    Err(e) => {
                        eprintln!(
                            "[warn] failed to decode block value ({} bytes) {}",
                            value.len(),
                            e
                        );
                    }
                }
            }
        });
    }
    drop(block_sender);

    let mut reported = 0;
    while let Some(m) = block_receiver.recv().await {
        block_size.push(m.total_size_kb);
        num_tx.push(m.num_transactions as f64);
        act_num_tx.push(m.act_num_transactions as f64);
        num_references.push(m.num_references as f64);
        reference_size.push(m.references_size_kb);
        reference_pct.push(m.references_size_kb / m.total_size_kb);
        overlap.push(m.overlap as f64);
        bitmap_size.push(m.bitmap_size as f64);
        tx_payload_size_per_block.push(m.payload_size_kb);
        if (m.per_tx_size > 0) {
            tx_size.push(m.per_tx_size as f64);
        }

        if m.round % 1000 == 0 {
            if m.round > reported {
                println!("Round {}", m.round);
            }
            reported = m.round
        }
    }

    let elapsed = start_time.elapsed();

    println!("elapsed: {:.2?}", elapsed);
    println!();
    let (size_mean, size_p1, size_p10, size_p50, size_p90, size_p99, size_max) = stats_summary(&block_size);
    let (tx_mean, tx1, tx10, tx_p50, tx_p90, tx_p99, tx_max) = stats_summary(&num_tx);
    let (act_tx_mean, act_tx1, act_tx10, act_tx_p50, act_tx_p90, act_tx_p99, act_tx_max) = stats_summary(&act_num_tx);

    let (ref_mean, ref1, ref10, ref_p50, ref_p90, ref_p99, ref_max) = stats_summary(&num_references);
    let (ref_size_mean, ref_size_1, ref_size_10, ref_size_50, ref_size_90, ref_size_99, ref_size_max) = stats_summary(&reference_size);
    let (ref_pct_mean, ref_pct_1, ref_pct_10, ref_pct_50, ref_pct_90, ref_pct_99, ref_pct_max) = stats_summary(&reference_pct);
    let (overlap_mean, overlap_1, overlap_10, overlap_50, overlap_90, overlap_99, overlap_max) = stats_summary(&overlap);
    let (tx_payload_size, ..) = stats_summary(&tx_payload_size_per_block);
    let (tx_size_mean, tx_size_1, tx_size_10, tx_size_50, tx_size_90, tx_size_99, tx_size_max) = stats_summary(&tx_size);
    let (bitmap_size_mean, bitmap_size_1, bitmap_size_10, bitmap_size_50, bitmap_size_90, bitmap_size_99, bitmap_size_max) = stats_summary(&bitmap_size);

    println!(
        "block_size (KB):             mean={:.3}  p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3}",
        size_mean, size_p1, size_p10, size_p50, size_p90, size_p99
    );
    println!(
        "num_transactions:      mean={:.3} p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3}",
        tx_mean, tx1, tx10, tx_p50, tx_p90, tx_p99
    );
    println!(
        "actual num_transactions:      mean={:.3} p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3}",
        act_tx_mean, act_tx1, act_tx10, act_tx_p50, act_tx_p90, act_tx_p99
    );
    println!("tx_payload_size_per_block (KB):          mean={:.3}", tx_payload_size);
    println!(
        "tx_size:        mean={:.3}  p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3}",
        tx_size_mean, tx_size_1, tx_size_10, tx_size_50, tx_size_90, tx_size_99
    );
    println!(
        "num_references:        mean={:.3}  p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3} max={:.3}",
        ref_mean, ref1, ref10, ref_p50, ref_p90, ref_p99, ref_max
    );
    println!("references_size (KB):  mean={:.3}  p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3}", ref_size_mean, ref_size_1, ref_size_10, ref_size_50, ref_size_90, ref_size_99);
    println!("% of total:            mean={:.5} p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3}", ref_pct_mean, ref_pct_1, ref_pct_10, ref_pct_50, ref_pct_90, ref_pct_99);
    println!("overlap:               mean={:.3} p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3} max={:.3}", overlap_mean, overlap_1, overlap_10, overlap_50, overlap_90, overlap_99, overlap_max);
    println!("bitmap-size:           mean={:.3} p1={:.3} p10={:.3} p50={:.3}  p90={:.3}  p99={:.3} max={:.3}", bitmap_size_mean, bitmap_size_1, bitmap_size_10, bitmap_size_50, bitmap_size_90, bitmap_size_99, bitmap_size_max);

    std::process::exit(0);
}
