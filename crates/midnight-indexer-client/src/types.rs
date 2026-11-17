use serde::{Deserialize, Serialize};

use crate::error::IndexerError;

// ---------------------------------------------------------------------------
// GraphQL envelope
// ---------------------------------------------------------------------------

/// Wrapper for GraphQL response envelope.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct GraphQLResponse<T> {
    pub data: Option<T>,
    #[serde(default)]
    pub errors: Option<Vec<GraphQLError>>,
}

impl<T> GraphQLResponse<T> {
    /// Returns the `data`, or [`IndexerError::GraphQL`] when `errors` is non-empty.
    ///
    /// The errors win over a partial result that carries both.
    pub(crate) fn into_result(self) -> Result<Option<T>, IndexerError> {
        match self.errors {
            Some(errors) if !errors.is_empty() => Err(IndexerError::GraphQL(errors)),
            _ => Ok(self.data),
        }
    }
}

/// A single GraphQL error from the response.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GraphQLError {
    pub message: String,
}

impl std::fmt::Display for GraphQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

// ---------------------------------------------------------------------------
// Block
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub hash: String,
    pub height: i64,
    #[serde(default)]
    pub protocol_version: Option<i64>,
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default)]
    pub ledger_parameters: Option<String>,
}

// ---------------------------------------------------------------------------
// Query offset types (mirror GraphQL schema inputs)
// ---------------------------------------------------------------------------

/// Offset for block queries: fetch by height or hash.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum BlockOffset {
    Height { height: i64 },
    Hash { hash: String },
}

impl BlockOffset {
    pub fn height(h: i64) -> Self {
        Self::Height { height: h }
    }

    pub fn hash(h: impl Into<String>) -> Self {
        Self::Hash { hash: h.into() }
    }
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

/// Offset for transaction queries: the transaction's hash (32 bytes, 64 hex
/// characters) or one of its identifiers (66 hex characters).
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum TransactionOffset {
    Hash { hash: String },
    Identifier { identifier: String },
}

/// A transaction the indexer lists, which it does only once its block is final.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IndexedTransaction {
    /// `RegularTransaction` or `SystemTransaction`.
    #[serde(rename = "__typename")]
    pub kind: String,
    pub hash: String,
    pub block: TransactionBlock,
    /// Regular transactions only.
    #[serde(default)]
    pub identifiers: Option<Vec<String>>,
    /// The fee paid in specks, as a decimal string. Regular transactions only;
    /// a failed one pays it too.
    #[serde(default)]
    pub fee: Option<String>,
    /// Regular transactions only.
    #[serde(default)]
    pub transaction_result: Option<TransactionResult>,
}

/// The block a listed transaction is in. `timestamp` is in milliseconds.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TransactionBlock {
    pub height: i64,
    pub hash: String,
    pub timestamp: i64,
}

/// How the ledger applied a regular transaction.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TransactionResult {
    /// `SUCCESS`, `PARTIAL_SUCCESS` or `FAILURE`.
    pub status: String,
    /// Each segment's success; segment 0 is the guaranteed section.
    #[serde(default)]
    pub segments: Option<Vec<SegmentResult>>,
}

/// One segment's success within a transaction.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SegmentResult {
    pub id: i64,
    pub success: bool,
}

// ---------------------------------------------------------------------------
// Query variable structs (used internally by IndexerClient)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Default)]
pub(crate) struct BlockQueryVars {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<BlockOffset>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TransactionQueryVars {
    pub offset: TransactionOffset,
}

// ---------------------------------------------------------------------------
// Query-specific response wrappers
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct BlockQueryData {
    pub block: Option<Block>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct TransactionQueryData {
    pub transactions: Vec<IndexedTransaction>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_block() {
        let json = r#"{
            "hash": "abc123",
            "height": 42,
            "protocolVersion": 1,
            "timestamp": 1700000000,
            "author": "node-1",
            "ledgerParameters": null
        }"#;

        let block: Block = serde_json::from_str(json).unwrap();
        assert_eq!(block.hash, "abc123");
        assert_eq!(block.height, 42);
        assert_eq!(block.protocol_version, Some(1));
        assert_eq!(block.timestamp, Some(1700000000));
        assert_eq!(block.author, Some("node-1".to_string()));
    }

    #[test]
    fn graphql_errors_win_over_partial_data() {
        let partial = GraphQLResponse {
            data: Some(1),
            errors: Some(vec![GraphQLError {
                message: "boom".into(),
            }]),
        };
        assert!(matches!(
            partial.into_result(),
            Err(IndexerError::GraphQL(errors)) if errors[0].message == "boom"
        ));

        let no_errors = GraphQLResponse {
            data: Some(1),
            errors: Some(vec![]),
        };
        assert!(matches!(no_errors.into_result(), Ok(Some(1))));
    }

    /// The indexer's reply for a localnet send (ledger 9), as it sent it.
    #[test]
    fn deserialize_a_listed_transaction() {
        let body = r#"{"transactions":[{"__typename":"RegularTransaction","hash":"22094f4eb6c32e567eb116e98b3ead6393aeb25ca57d9cfbc4b89cf9dcf3ef98","identifiers":["008e540745728d99457c08206981e32e5755dc34505fbcbf4c84c406e4962b1d64"],"fee":"499169974095779","transactionResult":{"status":"SUCCESS","segments":null},"block":{"height":144071,"hash":"feb56007171b0c095627835e70aa38f2de4982e86e881f36da2cb30f7204aad0","timestamp":1794886518002}}]}"#;
        let data: TransactionQueryData = serde_json::from_str(body).unwrap();
        assert_eq!(
            data.transactions,
            vec![IndexedTransaction {
                kind: "RegularTransaction".into(),
                hash: "22094f4eb6c32e567eb116e98b3ead6393aeb25ca57d9cfbc4b89cf9dcf3ef98".into(),
                block: TransactionBlock {
                    height: 144_071,
                    hash: "feb56007171b0c095627835e70aa38f2de4982e86e881f36da2cb30f7204aad0".into(),
                    timestamp: 1_794_886_518_002,
                },
                identifiers: Some(vec![
                    "008e540745728d99457c08206981e32e5755dc34505fbcbf4c84c406e4962b1d64".into()
                ]),
                fee: Some("499169974095779".into()),
                transaction_result: Some(TransactionResult {
                    status: "SUCCESS".into(),
                    segments: None
                }),
            }]
        );
        let none: TransactionQueryData = serde_json::from_str(r#"{"transactions":[]}"#).unwrap();
        assert!(none.transactions.is_empty());
    }

    /// A system transaction carries none of the regular fields, and a partial success lists its segments.
    #[test]
    fn deserialize_a_system_transaction_and_a_partial_success() {
        let system: IndexedTransaction = serde_json::from_str(
            r#"{"__typename":"SystemTransaction","hash":"ab","block":{"height":1,"hash":"cd","timestamp":0}}"#,
        )
        .unwrap();
        assert_eq!(
            (system.identifiers, system.fee, system.transaction_result),
            (None, None, None)
        );
        let partial: TransactionResult =
            serde_json::from_str(r#"{"status":"PARTIAL_SUCCESS","segments":[{"id":0,"success":true},{"id":1,"success":false}]}"#)
                .unwrap();
        assert_eq!(
            partial.segments,
            Some(vec![
                SegmentResult {
                    id: 0,
                    success: true
                },
                SegmentResult {
                    id: 1,
                    success: false
                }
            ])
        );
    }

    /// The offset is the schema's `TransactionOffset` input: one of `hash` or `identifier`.
    #[test]
    fn a_transaction_offset_names_one_field() {
        let vars = TransactionQueryVars {
            offset: TransactionOffset::Hash { hash: "ab".into() },
        };
        assert_eq!(
            serde_json::to_value(vars).unwrap(),
            serde_json::json!({"offset": {"hash": "ab"}})
        );
        let vars = TransactionQueryVars {
            offset: TransactionOffset::Identifier {
                identifier: "cd".into(),
            },
        };
        assert_eq!(
            serde_json::to_value(vars).unwrap(),
            serde_json::json!({"offset": {"identifier": "cd"}})
        );
    }
}
