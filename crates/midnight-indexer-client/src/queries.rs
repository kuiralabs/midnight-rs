/// Fetch a block by optional offset (hash or height). Returns latest if no offset.
pub const BLOCK_QUERY: &str = r#"
    query BlockQuery($offset: BlockOffset) {
        block(offset: $offset) {
            hash
            height
            protocolVersion
            timestamp
            author
            ledgerParameters
        }
    }
"#;

/// Fetch the transaction with a hash or one of its identifiers. The list is
/// empty when the indexer does not have it.
pub const TRANSACTION_QUERY: &str = r#"
    query TransactionQuery($offset: TransactionOffset!) {
        transactions(offset: $offset) {
            __typename
            hash
            block { height hash timestamp }
            ... on RegularTransaction {
                identifiers
                fee
                transactionResult { status segments { id success } }
            }
        }
    }
"#;

/// The transactions that spend shielded coins whose nullifiers start with one
/// of `$prefixes` (ten at most), from `$fromBlock` to `$toBlock`. With
/// `$toBlock` at or below the indexer's latest block, the stream completes
/// once it has sent them all.
pub const SHIELDED_NULLIFIER_SPENDS_SUBSCRIPTION: &str = r#"
    subscription ShieldedNullifierSpends($prefixes: [HexEncoded!]!, $fromBlock: Int, $toBlock: Int) {
        shieldedNullifierTransactions(nullifierPrefixes: $prefixes, fromBlock: $fromBlock, toBlock: $toBlock) {
            nullifier
            transactionHash
            blockHeight
        }
    }
"#;
