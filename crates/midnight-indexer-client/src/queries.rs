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
