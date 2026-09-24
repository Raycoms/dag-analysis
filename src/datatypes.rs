use serde::Deserialize;

pub type Round = u32;
pub type AuthorityIndex = u32;
pub type Epoch = u64;
pub type BlockTimestampMs = u64;
pub type CommitIndex = u32;
pub type TransactionIndex = u16;

#[derive(Deserialize, Clone, Copy)]
pub struct BlockDigest([u8; 32]);

#[derive(Deserialize, Clone, Copy)]
pub struct BlockRef {
    round: Round,
    pub(crate) author: AuthorityIndex,
    digest: BlockDigest,
}

#[derive(Deserialize)]
pub struct Transaction {
    pub data: Vec<u8>,
}

#[derive(Deserialize)]
pub struct CommitDigest([u8; 32]);

#[derive(Deserialize)]
pub struct CommitVote {
    index: CommitIndex,
    digest: CommitDigest,
}

#[derive(Deserialize)]
pub enum MisbehaviorProof {
    InvalidBlock(#[allow(dead_code)] BlockRef),
}

#[derive(Deserialize)]
pub struct MisbehaviorReport {
    target: AuthorityIndex,
    proof: MisbehaviorProof,
}

#[derive(Deserialize)]
pub struct BlockTransactionVotes {
    #[allow(dead_code)]
    block_ref: BlockRef,
    #[allow(dead_code)]
    rejects: Vec<TransactionIndex>,
}

#[derive(Deserialize)]
pub struct BlockV1 {
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
pub struct BlockV2 {
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
pub struct BlockV3 {
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
pub enum Block {
    V1(BlockV1),
    V2(BlockV2),
    V3(BlockV3),
}

#[derive(Deserialize)]
pub struct SignedBlock {
    pub(crate) inner: Block,
    signature: Vec<u8>,
}

pub fn parse_block_value(value_bytes: &[u8]) -> bcs::Result<SignedBlock> {
    let inner_bytes: Vec<u8> = bcs::from_bytes(value_bytes)?;
    bcs::from_bytes(&inner_bytes)
}

pub struct BlockMetrics {
    pub(crate) total_size_kb: f64,
    pub(crate) num_references: u32,
    pub(crate) references_size_kb: f64,
    pub(crate) num_transactions: u32,
    pub(crate) payload_size_kb: f64,
    pub(crate) overlap: i64,
    pub(crate) round: u32,
    pub(crate) per_tx_size: u32,
    pub(crate) bitmap_size: u32,
    pub(crate) act_num_transactions: u32,
}

pub fn common_fields(
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