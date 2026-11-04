//! What a wallet's sync maintains, as its consumers read it: the unshielded
//! UTXOs it tracks, the cursors that say how far it has reached, and the
//! progress it reports on the way.

use midnight_helpers::HashOutput;

use crate::{NIGHT, SpentUtxoKey, UnshieldedTokenType};

/// How far a wallet's sync has reached. See `WalletFacade::sync_cursors`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncCursors {
    /// Height of the latest block seen in an unshielded transaction event.
    ///
    /// This is NOT a general chain-sync cursor. It only advances when the
    /// wallet's unshielded address appears in a transaction.
    pub last_block_height: i64,
    /// Indexer id of the latest transaction the wallet applied.
    pub last_tx_id: Option<i64>,
    /// Highest zswap event id the wallet applied.
    pub zswap_event_id: i64,
    /// Highest dust event id the wallet applied.
    pub dust_event_id: i64,
}

/// A tracked unshielded UTXO from the indexer.
#[derive(Debug, Clone)]
pub struct TrackedUtxo {
    pub owner: String,
    /// The token this UTXO holds.
    pub token_type: UnshieldedTokenType,
    pub value: u128,
    /// The intent hash of the transaction that created this UTXO.
    pub intent_hash: HashOutput,
    /// The number of this UTXO among its transaction's outputs, as the ledger
    /// counts it.
    pub output_index: u32,
    /// Creation time in seconds since the epoch. Dust generation accrues from
    /// this instant, so a dust registration needs it to declare the fee
    /// allowance the ledger will accept.
    pub ctime: Option<i64>,
    /// Whether this UTXO already generates dust. The ledger grants no
    /// generationless availability for such a UTXO, so a registration must
    /// leave it out.
    pub registered_for_dust_generation: Option<bool>,
}

impl TrackedUtxo {
    /// Whether this UTXO holds the native NIGHT token.
    pub fn is_night(&self) -> bool {
        self.token_type == NIGHT
    }

    /// Whether this UTXO already generates dust.
    ///
    /// An absent flag reads as not registered, which is what makes a
    /// registration build include the UTXO. Read it through here rather than
    /// comparing the field, so a caller asking "is this registered" and the
    /// builder asking "does this still need registering" cannot disagree.
    pub fn is_registered_for_dust(&self) -> bool {
        self.registered_for_dust_generation == Some(true)
    }

    /// The key that a reservation of this UTXO and a confirmed spend of it
    /// share, so that the confirmation clears the reservation.
    pub fn spent_key(&self) -> SpentUtxoKey {
        SpentUtxoKey {
            intent_hash: self.intent_hash,
            output_index: self.output_index,
        }
    }
}

/// Progress updates emitted during wallet sync.
#[derive(Debug, Clone)]
pub enum SyncProgress {
    /// The sync resumes from a stored snapshot.
    Resuming {
        /// The id of the last zswap event that the snapshot applied.
        zswap_event_id: i64,
        /// The id of the last Dust event that the snapshot read.
        dust_event_id: i64,
    },
    /// The zswap replay applied another 10,000 events.
    ZswapEvents {
        /// The id of the last zswap event that the replay applied.
        current: i64,
        /// The highest zswap event id that the indexer reports.
        max: i64,
    },
    /// The zswap replay reached the highest event id that the indexer
    /// reports.
    ZswapComplete {
        /// The number of zswap events that the replay applied after the last
        /// hard fork that it crossed, or all of them when it crossed none. It
        /// is 0 when the wallet was at the tip.
        events: u64,
    },
    /// The Dust replay read another 10,000 new events.
    DustEvents {
        /// The id of the last Dust event that the replay read.
        current: i64,
        /// The highest Dust event id that the indexer reports.
        max: i64,
    },
    /// The Dust replay reached the highest event id that the indexer reports.
    DustComplete {
        /// The number of new Dust events that the replay read, 0 when the
        /// wallet was at the tip. It includes the events of an earlier ledger
        /// generation, which the replay skips.
        events: u64,
    },
    /// The unshielded replay reached the last transaction that the indexer
    /// reports for the wallet's address.
    UnshieldedCaughtUp {
        /// The number of unspent UTXOs that the wallet holds at its address.
        utxos: usize,
    },
}

/// Where a long-running replay reports its [`SyncProgress`].
///
/// A plain callback rather than a channel, so a trait that hands one over
/// does not tie its implementations to an async runtime.
pub type ProgressSink = std::sync::Arc<dyn Fn(SyncProgress) + Send + Sync>;
