//! A synced wallet on whichever ledger generation its chain runs, and the
//! sync that carries it across a hard fork.

use std::path::{Path, PathBuf};

use midnight_helpers::DefaultDB;
use midnight_indexer_client::{IndexerEndpoint, SubscriptionClient};
use midnight_types::chain_pin::ChainPin;
use midnight_types::hd::Seed;
use midnight_types::{
    ChainParameters, CoinInfo, CoinPublicKey, EncryptionPublicKey, LedgerVersion, Network,
    SpendableShieldedCoin, SpentInputs, SyncCursors, TrackedUtxo, WalletBalance, WalletError,
};
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::replay::{
    UnshieldedReplay, latest_block, ledger_of, progress_cancelled, replay_unshielded_events,
    send_progress,
};
use crate::storage::wallet_storage_id;
use crate::sync::SyncPlan;
use crate::{SyncProgress, ledger_8, ledger_9};

/// A Midnight wallet: identity (seed, addresses) and synced ledger state.
///
/// Maintains three streams of state from the indexer:
/// - `zswapLedgerEvents` → shielded coin tracking + Merkle tree
/// - `dustLedgerEvents` → dust/fee UTXO tracking
/// - `unshieldedTransactions` → unshielded UTXO balance
///
/// The state is in the ledger generation its chain runs
/// ([`Self::ledger_version`]). A sync reads the generation from the chain.
/// A sync or resync that meets a hard fork carries the wallet across it. The
/// shielded state continues in the new generation, and the Dust state starts
/// empty, because the fork emptied the chain's Dust state.
pub struct Wallet {
    seed: Seed,
    state: State,
}

/// The wallet's state, in its generation.
pub(crate) enum State {
    Ledger8(ledger_8::Wallet),
    Ledger9(ledger_9::Wallet),
}

/// Evaluate `$body` with `$w` bound to the state of whichever generation
/// `$state` holds.
macro_rules! each {
    ($state:expr, $w:ident => $body:expr) => {
        match $state {
            State::Ledger8($w) => $body,
            State::Ledger9($w) => $body,
        }
    };
}

impl Wallet {
    pub(crate) fn state(&self) -> &State {
        &self.state
    }

    pub(crate) fn state_mut(&mut self) -> &mut State {
        &mut self.state
    }

    /// Default storage directory: `~/.midnight/wallets/`
    pub fn default_storage_dir() -> Option<PathBuf> {
        std::env::home_dir().map(|h| h.join(".midnight").join("wallets"))
    }

    /// The ledger generation this wallet's state is in.
    pub fn ledger_version(&self) -> LedgerVersion {
        match self.state {
            State::Ledger8(_) => LedgerVersion::V8,
            State::Ledger9(_) => LedgerVersion::V9,
        }
    }

    /// The chain pin a snapshot on disk carries, or `None` when there is no
    /// snapshot or it predates the pin.
    ///
    /// Read this before a resume and judge it with
    /// [`crate::chain_pin::verify_pin`], against what the node reports for
    /// that height. A snapshot from a chain that no longer exists still
    /// resumes cleanly and reports the old balance, so nothing later in the
    /// sync will catch it.
    pub fn stored_chain_pin(
        storage_dir: &Path,
        network: impl Into<Network>,
        address: &str,
    ) -> Result<Option<ChainPin>, WalletError> {
        let network = network.into();
        crate::storage::load_chain_pin(storage_dir, network.as_str(), &wallet_storage_id(address))
    }

    /// Where the snapshot for `address` lives, so an error can name what to
    /// remove.
    pub fn snapshot_path(
        storage_dir: &Path,
        network: impl Into<Network>,
        address: &str,
    ) -> PathBuf {
        let network = network.into();
        crate::storage::snapshot_path(storage_dir, network.as_str(), &wallet_storage_id(address))
    }

    /// The sync that [`crate::WalletBuilder::sync`] and
    /// [`crate::WalletBuilder::sync_with_progress`] run.
    ///
    /// Reads the generation the chain runs from its latest block, resumes
    /// from a snapshot under `storage_dir` when there is one, and replays the
    /// three event streams, crossing each hard fork they meet. Checkpoints
    /// Dust progress to disk periodically, so an interrupted sync resumes
    /// where it left off.
    pub(crate) async fn sync_inner(
        plan: SyncPlan,
        progress: Option<mpsc::Sender<SyncProgress>>,
    ) -> Result<Self, WalletError> {
        let sync = ChainSync {
            indexer: &plan.indexer,
            seed: &plan.seed,
            address: &plan.address,
            network: &plan.network,
            storage_dir: plan.storage_dir.as_deref(),
            save: true,
            chain_pin: plan.chain_pin,
        };
        // A chain can fork between the read of its latest block and the
        // replay of its events. The replay then meets the later generation,
        // and a second attempt reads the block again and starts from there.
        let state = match sync_once(sync.clone(), progress.clone()).await {
            Err(WalletError::LedgerMismatch { expected, found }) if found > expected => {
                info!(%found, "the chain moved to a later ledger during the sync; syncing again");
                sync_once(sync, progress).await?
            }
            result => result?,
        };
        Ok(Self {
            seed: plan.seed,
            state,
        })
    }

    /// Whether the dust state has been synced (required for transaction
    /// building).
    pub fn dust_synced(&self) -> bool {
        each!(&self.state, w => w.dust_synced())
    }

    /// Hand back the inputs a build reserved, because that build will never
    /// reach the chain.
    ///
    /// Reserving on build stops a later build re-selecting the same inputs, so
    /// a transaction that is rejected at submit, or built and then abandoned,
    /// holds its coins until the TTL window elapses. Releasing returns them at
    /// once. Pass what the build reported spending.
    ///
    /// Only call this for a transaction that cannot land. Releasing one that is
    /// still in flight lets a later build re-select the same inputs, and the
    /// loser is rejected on chain.
    ///
    /// Persistence is best-effort, since the in-memory release already frees
    /// the inputs for this process.
    pub fn release(&mut self, spent: &SpentInputs) {
        each!(&mut self.state, w => w.release(spent))
    }

    /// Save the current wallet state to disk.
    ///
    /// Writes the confirmed-state files (`metadata.json`, `zswap-N.bin`,
    /// `dust_wallet-N.bin`) and the in-flight reservations to a separate
    /// `pending.json`. Confirmed and pending live in distinct files so a
    /// failed save of one does not corrupt the other. Runs automatically at
    /// the end of initial sync and after every successful resync when a
    /// storage directory is configured; calling it manually is only needed
    /// for extra checkpoints.
    pub fn save(&self, base: &Path) -> Result<(), WalletError> {
        each!(&self.state, w => w.save(base))
    }

    /// Where this wallet's snapshot lives, when it persists one.
    ///
    /// An error that tells a reader to remove the snapshot has to name it, so
    /// this is the path that goes in the message.
    pub fn snapshot_dir(&self) -> Option<PathBuf> {
        each!(&self.state, w => w.snapshot_dir())
    }

    /// The finalized block this wallet is pinned to, held in memory.
    ///
    /// A resume checks the pin on disk; a wallet that stays attached has to
    /// check this one, because its cursors go just as stale when the chain is
    /// replaced underneath it.
    pub fn chain_pin(&self) -> Option<&ChainPin> {
        each!(&self.state, w => w.chain_pin())
    }

    /// Move the pin to the block a fresh check saw, so it stays inside an
    /// archive's retention window rather than ageing out of it.
    pub fn set_chain_pin(&mut self, pin: ChainPin) {
        each!(&mut self.state, w => w.set_chain_pin(pin))
    }

    /// How far the sync has reached, as one snapshot.
    ///
    /// The four cursors advance together during a sync, so reading them one
    /// at a time can report a mixture of two syncs.
    pub fn sync_cursors(&self) -> SyncCursors {
        each!(&self.state, w => w.sync_cursors())
    }

    /// The seed this wallet signs and derives with.
    pub fn seed(&self) -> &Seed {
        &self.seed
    }

    /// The wallet's shielded public keys: the coin public key an output
    /// commits to, and the encryption key its discovery ciphertext is sealed
    /// to.
    ///
    /// Public material, so anything that only needs to address a coin to this
    /// wallet can take these instead of the seed. A signer that never releases
    /// its seed can still supply them.
    pub fn shielded_public_keys(&self) -> (CoinPublicKey, EncryptionPublicKey) {
        each!(&self.state, w => w.shielded_public_keys())
    }

    /// The network this wallet derives addresses for. [`Network::as_str`]
    /// gives its literal name in the bech32 HRP.
    pub fn network(&self) -> &Network {
        each!(&self.state, w => w.network())
    }

    /// The wallet's unshielded receiving address.
    pub fn unshielded_address(&self) -> String {
        each!(&self.state, w => w.unshielded_address())
    }

    /// The wallet's shielded receiving address, e.g. `mn_shield-addr_undeployed1...`.
    pub fn shielded_address(&self) -> String {
        self.seed.shielded_address(self.network())
    }

    /// The wallet's Dust address, e.g. `mn_dust_undeployed1...`. Another
    /// wallet can register its tNIGHT to it, so that this wallet gets the
    /// Dust that tNIGHT generates. See [`Seed::dust_address`].
    pub fn dust_address(&self) -> String {
        self.seed.dust_address(self.network())
    }

    /// The unshielded UTXOs this wallet tracks.
    pub fn unshielded_utxos(&self) -> &[TrackedUtxo] {
        each!(&self.state, w => w.unshielded_utxos())
    }

    /// The chain's Dust and TTL parameters, as this wallet last synced them.
    pub fn parameters(&self) -> ChainParameters {
        each!(&self.state, w => w.chain_parameters())
    }

    pub fn balance(&self) -> WalletBalance {
        each!(&self.state, w => w.balance())
    }

    /// Enumerate the wallet's spendable shielded coins, each with its full
    /// coin info (nonce, token type, value) plus the nullifier that pins it.
    ///
    /// Use this to address a specific coin for a circuit that spends it (e.g.
    /// `receiveShielded`): build the `ShieldedCoinInfo` argument from the
    /// coin's `nonce`/`token_type`/`value`, then hand the same coin back to the
    /// call builder so the SDK spends that exact coin as the shielded input.
    /// Coins a still-pending build already spent are left out.
    pub fn spendable_shielded_coins(&self) -> Vec<SpendableShieldedCoin> {
        each!(&self.state, w => w.spendable_shielded_coins())
    }

    /// Register a coin this wallet owns but cannot discover, so a replay
    /// claims it without decrypting anything.
    ///
    /// A shielded coin normally reaches its owner through the discovery
    /// ciphertext on its output. That channel fails when the party that built
    /// the transaction attached no ciphertext, or sealed it to a key this
    /// wallet does not hold. The coin is still owned by this wallet's coin
    /// public key and still spendable, as long as its owner can rebuild the
    /// `CoinInfo` (nonce, token type, value) from somewhere else. A contract
    /// that evolves one public nonce per mint is such a case.
    ///
    /// Registration records the coin's commitment under this wallet's coin
    /// public key. A replay that meets the matching output claims the coin
    /// from that record.
    ///
    /// **Registration alone does not recover a coin whose output the wallet
    /// already replayed past.** A replay that meets an output it cannot claim
    /// collapses that Merkle leaf, and a resync resumes from the cursor
    /// instead of revisiting it. Register through the handle instead:
    /// [`WalletFacade::watch_for_coins`](crate::WalletFacade::watch_for_coins)
    /// on a [`LocalWallet`](crate::LocalWallet) registers, then replays the
    /// stream from its first event. `MidnightProvider::watch_for_coin` calls
    /// it.
    ///
    /// A registration is part of the wallet state, so it is persisted with it
    /// when a storage directory is configured, and a coin registered before
    /// it lands on chain survives a restart. The in-memory registration
    /// stands even when that save fails.
    pub fn watch_for_coin(&mut self, coin: CoinInfo) -> Result<(), WalletError> {
        each!(&mut self.state, w => w.watch_for_coin(coin))
    }

    /// [`Self::watch_for_coin`] for several coins, persisting once.
    pub fn watch_for_coins(
        &mut self,
        coins: impl IntoIterator<Item = CoinInfo>,
    ) -> Result<(), WalletError> {
        each!(&mut self.state, w => w.watch_for_coins(coins))
    }

    /// Drop a registration [`Self::watch_for_coin`] made, for a coin that
    /// turned out to be wrong.
    ///
    /// A registration whose `CoinInfo` does not match the coin an on-chain
    /// output commits to never matches anything, so it would otherwise ride
    /// along on every later replay.
    ///
    /// This drops the registration only. A coin the wallet already claimed is
    /// untouched, and stays spendable: it is held now, not watched for.
    /// Forgetting a coin that was never registered changes no registration.
    /// When the last save failed, this call still saves, and can return
    /// [`WalletError::Storage`].
    pub fn forget_coin(&mut self, coin: CoinInfo) -> Result<(), WalletError> {
        each!(&mut self.state, w => w.forget_coin(coin))
    }

    /// [`Self::forget_coin`] for several coins, persisting once.
    pub fn forget_coins(
        &mut self,
        coins: impl IntoIterator<Item = CoinInfo>,
    ) -> Result<(), WalletError> {
        each!(&mut self.state, w => w.forget_coins(coins))
    }

    /// Snapshot the inputs of a resync's replay phase. See [`ResyncPlan`].
    pub(crate) fn resync_plan(&self) -> ResyncPlan {
        let kind = match &self.state {
            State::Ledger8(w) => ResyncPlanKind::Ledger8 {
                plan: w.resync_plan(),
                identity: WalletIdentity {
                    seed: self.seed.clone(),
                    network: w.network().clone(),
                    storage_dir: w.storage_dir().map(Path::to_path_buf),
                    chain_pin: w.chain_pin().cloned(),
                },
            },
            State::Ledger9(w) => ResyncPlanKind::Ledger9(w.resync_plan()),
        };
        ResyncPlan {
            indexer: each!(&self.state, w => w.indexer()).clone(),
            kind,
        }
    }

    /// Snapshot the inputs of a rebuild from the chain's first event. See
    /// [`ResyncPlan`].
    ///
    /// The plan carries none of the replayed state, only who the wallet is
    /// and the coins a state rebuilt from scratch must be told about (see
    /// [`Self::shielded_rescan_plan`]). The rebuild then runs the sync a
    /// first sync runs, from the earliest generation this build links, and
    /// crosses each hard fork the chain's history holds.
    pub(crate) fn resync_plan_from_genesis(&self) -> ResyncPlan {
        let (identity, address, coins) = each!(&self.state, w => (
            WalletIdentity {
                seed: self.seed.clone(),
                network: w.network().clone(),
                storage_dir: w.storage_dir().map(Path::to_path_buf),
                chain_pin: w.chain_pin().cloned(),
            },
            w.unshielded_address(),
            w.rescan_coins(),
        ));
        ResyncPlan {
            indexer: each!(&self.state, w => w.indexer()).clone(),
            kind: ResyncPlanKind::Genesis {
                identity,
                address,
                coins,
            },
        }
    }

    /// Apply validated resync results to `self`, and persist when they change
    /// durable state. A commit also persists when the last save failed.
    /// Separate from [`ResyncPlan::run`], which performs the I/O and
    /// validation, so this sequence is unit-testable without an indexer and
    /// so lock-sharing callers can scope their write lock to this call alone.
    ///
    /// Commit-time semantics, relevant when the wallet was mutated between
    /// [`Self::resync_plan`] and this call (callers must still prevent
    /// *concurrent resyncs*; see [`ResyncPlan::run`]):
    ///
    /// - Replay-derived state (`dust_wallet`, `zswap_state`,
    ///   `unshielded_utxos`) and the sync cursors are overwritten. With
    ///   resyncs serialized, the only state an overwrite could clobber is a
    ///   coin registration (see below): transfer builds record their
    ///   in-flight spends in the separate pending set and never touch
    ///   confirmed state.
    /// - Coin registrations ([`Self::watch_for_coin`]) live in `zswap_state`.
    ///   The replay starts from the plan's registrations, so the commit keeps
    ///   each one that the replay did not consume. A replay consumes a
    ///   registration when it claims the coin. The overwrite drops a
    ///   registration made after the plan snapshot, and brings back one
    ///   forgotten after it. So callers must not change a registration
    ///   between plan and commit, as [`LocalWallet`](crate::LocalWallet)
    ///   makes sure with its sync lock.
    /// - The pending reservation set is **merged, not overwritten**:
    ///   `clear_confirmed` drops exactly the entries whose spends this
    ///   replay observed on-chain, evaluated against the pending set as it
    ///   is *now*. Reservations added after the plan snapshot survive (their
    ///   spends cannot have been observed by a replay that started earlier).
    /// - `parameters` and `block_context` are refreshed from the chain view
    ///   the replay fetched.
    ///
    /// A resync that crossed a hard fork replaces the state with the later
    /// generation's and saves it. The shielded state crosses with its
    /// registrations, and the chain pin stays as the commit finds it. The
    /// reservations do not carry over: a transaction built for the earlier
    /// generation cannot land on the chain that left it.
    ///
    /// A rebuild from genesis replaces the state with the one its replay
    /// built, in whichever generation the chain now runs, and saves it. A
    /// rebuild in the wallet's own generation keeps the reservations, whose
    /// transactions may still land.
    pub(crate) fn commit_resync(&mut self, commit: ResyncCommit) -> Result<(), WalletError> {
        match (&mut self.state, commit.0) {
            (State::Ledger8(w), ResyncCommitKind::Ledger8(c)) => w.commit_resync(c),
            (State::Ledger9(w), ResyncCommitKind::Ledger9(c)) => w.commit_resync(c),
            (State::Ledger8(w8), ResyncCommitKind::Crossed(mut w9)) => {
                if let Some(pin) = w8.chain_pin() {
                    w9.set_chain_pin(pin.clone());
                }
                let saved = w9.persist(true);
                self.state = State::Ledger9(*w9);
                info!("the wallet crossed to ledger 9");
                saved
            }
            (_, ResyncCommitKind::Rebuilt(mut state)) => {
                match (&mut self.state, &mut *state) {
                    (State::Ledger8(old), State::Ledger8(new)) => new.adopt_pending(old),
                    (State::Ledger9(old), State::Ledger9(new)) => new.adopt_pending(old),
                    _ => {}
                }
                let saved = each!(&mut *state, w => w.persist(true));
                self.state = *state;
                info!(ledger = %self.ledger_version(), "the wallet was rebuilt from genesis");
                saved
            }
            (_, ResyncCommitKind::Ledger9(_)) => Err(WalletError::LedgerMismatch {
                expected: self.ledger_version(),
                found: LedgerVersion::V9,
            }),
            (_, ResyncCommitKind::Ledger8(_) | ResyncCommitKind::Crossed(_)) => {
                Err(WalletError::LedgerMismatch {
                    expected: self.ledger_version(),
                    found: LedgerVersion::V8,
                })
            }
        }
    }

    /// Snapshot the inputs of a shielded rescan's replay phase. See
    /// [`ShieldedRescanPlan`] for the intended plan → run → commit flow.
    pub(crate) fn shielded_rescan_plan(&self) -> ShieldedRescanPlan {
        let (plan, coins) = match &self.state {
            State::Ledger8(w) => (
                RescanPlanKind::Ledger8(w.shielded_rescan_plan()),
                w.rescan_coins(),
            ),
            State::Ledger9(w) => (
                RescanPlanKind::Ledger9(w.shielded_rescan_plan()),
                w.rescan_coins(),
            ),
        };
        ShieldedRescanPlan {
            indexer: each!(&self.state, w => w.indexer()).clone(),
            seed: self.seed.clone(),
            coins,
            plan,
        }
    }

    /// Apply a completed shielded rescan to `self` and persist it when a
    /// storage directory is configured.
    ///
    /// The rebuilt shielded state and its cursor replace what the wallet
    /// holds. The plan seeds the replay with every registration, so the
    /// commit keeps each one that the replay did not consume, and callers
    /// must not change a registration between plan and commit, as for
    /// [`Self::commit_resync`]. Callers must keep resyncs from interleaving
    /// (see [`ShieldedRescanPlan::run`]). Everything else a resync writes
    /// (dust, unshielded, parameters, block context, pending reservations) is
    /// left alone here.
    pub(crate) fn commit_shielded_rescan(
        &mut self,
        commit: ShieldedRescanCommit,
    ) -> Result<(), WalletError> {
        match (&mut self.state, commit.0) {
            (State::Ledger8(w), RescanCommitKind::Ledger8(c)) => w.commit_shielded_rescan(c),
            (State::Ledger9(w), RescanCommitKind::Ledger9(c)) => w.commit_shielded_rescan(c),
            (State::Ledger8(_), RescanCommitKind::Ledger9(_)) => Err(WalletError::LedgerMismatch {
                expected: LedgerVersion::V8,
                found: LedgerVersion::V9,
            }),
            (State::Ledger9(_), RescanCommitKind::Ledger8(_)) => Err(WalletError::LedgerMismatch {
                expected: LedgerVersion::V9,
                found: LedgerVersion::V8,
            }),
        }
    }
}

/// Snapshot of everything a resync's replay phase consumes, taken from a
/// `&Wallet` by [`Wallet::resync_plan`], with the indexer the wallet synced
/// from.
///
/// Exists so callers that share a wallet across tasks (notably
/// [`LocalWallet`](crate::LocalWallet)) can run the slow replay I/O while
/// reads and builds still reach the wallet: snapshot under a brief read lock,
/// [`ResyncPlan::run`] the replays with no read or write lock held, then apply
/// the validated result under a brief write lock via
/// [`Wallet::commit_resync`].
///
/// The fields are clones of the wallet's cursors and replay state; taking a
/// plan does not mutate or lock anything beyond the `&self` borrow.
#[must_use = "run the plan with ResyncPlan::run, then apply it with Wallet::commit_resync"]
pub(crate) struct ResyncPlan {
    indexer: IndexerEndpoint,
    kind: ResyncPlanKind,
}

enum ResyncPlanKind {
    Ledger8 {
        plan: ledger_8::ResyncPlan,
        identity: WalletIdentity,
    },
    Ledger9(ledger_9::ResyncPlan),
    /// A rebuild from the chain's first event.
    Genesis {
        identity: WalletIdentity,
        /// The wallet's unshielded address.
        address: String,
        /// The coins the rebuilt shielded state registers before its replay.
        coins: Vec<CoinInfo>,
    },
}

/// What a resync that crosses a hard fork needs beyond its plan, to build
/// the wallet again in the later generation.
struct WalletIdentity {
    seed: Seed,
    network: Network,
    storage_dir: Option<PathBuf>,
    chain_pin: Option<ChainPin>,
}

impl ResyncPlan {
    /// Run the resync's replay phase: resume the three indexer subscriptions
    /// from the snapshotted cursors and fetch the latest block (chain time +
    /// ledger parameters), all without touching the wallet.
    ///
    /// Returns the validated [`ResyncCommit`] to apply with
    /// [`Wallet::commit_resync`]. On any replay or fetch error nothing was
    /// committed anywhere, so the wallet the plan was taken from is
    /// untouched.
    ///
    /// Callers that release the wallet lock between plan and commit must
    /// serialize resyncs themselves (two concurrent runs would replay from
    /// the same cursors and race their commits).
    /// [`LocalWallet`](crate::LocalWallet) holds a mutex across plan → run →
    /// commit for this.
    ///
    /// When the chain moved to a later generation since the wallet's last
    /// sync, the replay crosses the fork. It applies the shielded events up
    /// to the fork and carries the shielded state across. Then it replays the
    /// Dust stream into an empty state of the later generation. It writes
    /// nothing, and the commit saves the crossed state.
    ///
    /// A rebuild from genesis reports to `progress` what a first sync
    /// reports, and like a first sync, it reads an indexer that sends nothing
    /// as a stall, not as the tip. A resync from cursors reports nothing.
    pub(crate) async fn run(
        self,
        progress: Option<mpsc::Sender<SyncProgress>>,
    ) -> Result<ResyncCommit, WalletError> {
        let ResyncPlan { indexer, kind } = self;
        let indexer = &indexer;
        match kind {
            ResyncPlanKind::Genesis {
                identity,
                address,
                coins,
            } => rebuild(&identity, &address, &coins, indexer, progress).await,
            ResyncPlanKind::Ledger9(plan) => Ok(ResyncCommit(ResyncCommitKind::Ledger9(
                plan.run(indexer).await?,
            ))),
            ResyncPlanKind::Ledger8 { plan, identity } => {
                let block = latest_block(indexer).await?;
                let block = if ledger_of(&block)? == LedgerVersion::V8 {
                    // The chain can fork before this replay ends, and the
                    // replay then meets the later generation's events.
                    match plan.clone().run(indexer).await {
                        Err(WalletError::LedgerMismatch { expected, found })
                            if found > expected =>
                        {
                            info!(%found, "the chain crossed a hard fork during the resync");
                            latest_block(indexer).await?
                        }
                        result => return Ok(ResyncCommit(ResyncCommitKind::Ledger8(result?))),
                    }
                } else {
                    block
                };
                cross(plan, identity, &block, indexer).await
            }
        }
    }
}

/// Rebuild a wallet from the chain's first event, as a first sync builds it.
///
/// The shielded state starts empty in the earliest generation this build
/// links, with `coins` registered, and the sync crosses each hard fork the
/// chain's history holds. Nothing is written: the commit saves the state.
async fn rebuild(
    identity: &WalletIdentity,
    address: &str,
    coins: &[CoinInfo],
    indexer: &IndexerEndpoint,
    progress: Option<mpsc::Sender<SyncProgress>>,
) -> Result<ResyncCommit, WalletError> {
    let sync = ChainSync {
        indexer,
        seed: &identity.seed,
        address,
        network: &identity.network,
        storage_dir: identity.storage_dir.as_deref(),
        save: false,
        chain_pin: identity.chain_pin.clone(),
    };
    let start = || {
        let registered = ledger_8::ShieldedRescanPlan::new(&identity.seed, coins).initial_state;
        Start::Genesis(Box::new(ZswapState::Ledger8(registered)))
    };
    let block = latest_block(indexer).await?;
    // A chain can fork between the read of its latest block and the replay,
    // as for a first sync.
    let state = match sync_on_chain(sync.clone(), &block, start(), progress.clone()).await {
        Err(WalletError::LedgerMismatch { expected, found }) if found > expected => {
            info!(%found, "the chain moved to a later ledger during the rebuild; rebuilding again");
            let block = latest_block(indexer).await?;
            sync_on_chain(sync, &block, start(), progress).await?
        }
        result => result?,
    };
    Ok(ResyncCommit(ResyncCommitKind::Rebuilt(Box::new(state))))
}

/// Replay a ledger 8 wallet's plan onto a chain that crossed to ledger 9,
/// and return the wallet again in ledger 9.
async fn cross(
    plan: ledger_8::ResyncPlan,
    identity: WalletIdentity,
    block: &midnight_indexer_client::Block,
    indexer: &IndexerEndpoint,
) -> Result<ResyncCommit, WalletError> {
    let ledger_8::ResyncPlan {
        unshielded_address,
        dust_wallet,
        dust_event_id,
        zswap_state,
        zswap_event_id,
        unshielded_utxos,
        last_tx_id,
        last_block_height,
        ..
    } = plan;
    let from = Resume {
        zswap: ZswapState::Ledger8(zswap_state),
        zswap_event_id,
        dust: DustState::Ledger8(dust_wallet),
        dust_event_id,
        unshielded_utxos,
        last_tx_id,
        last_block_height,
    };
    let state = sync_on_chain(
        ChainSync {
            indexer,
            seed: &identity.seed,
            address: &unshielded_address,
            network: &identity.network,
            storage_dir: identity.storage_dir.as_deref(),
            save: false,
            chain_pin: identity.chain_pin,
        },
        block,
        Start::Resume(Box::new(from)),
        None,
    )
    .await?;
    match state {
        State::Ledger9(wallet) => Ok(ResyncCommit(ResyncCommitKind::Crossed(Box::new(wallet)))),
        State::Ledger8(_) => Err(WalletError::LedgerMismatch {
            expected: LedgerVersion::V9,
            found: LedgerVersion::V8,
        }),
    }
}

/// Validated results of a resync's replay tasks and latest-block fetch,
/// ready to be committed into a [`Wallet`] via [`Wallet::commit_resync`].
///
/// Produced only by [`ResyncPlan::run`]; the fields are private so a commit
/// can't be forged from un-validated data. Grouping the commit inputs also
/// makes the commit-and-persist sequence unit-testable without a live
/// indexer.
#[must_use = "apply with Wallet::commit_resync, or the completed replay is discarded"]
pub(crate) struct ResyncCommit(ResyncCommitKind);

enum ResyncCommitKind {
    Ledger8(ledger_8::ResyncCommit),
    Ledger9(ledger_9::ResyncCommit),
    /// A ledger 8 wallet's resync that crossed the fork to ledger 9: the
    /// wallet again, in ledger 9.
    Crossed(Box<ledger_9::Wallet>),
    /// A rebuild from genesis: the wallet again, in the generation the chain
    /// runs.
    Rebuilt(Box<State>),
}

/// Snapshot of what a shielded rescan's replay consumes, taken from a
/// `&Wallet` by [`Wallet::shielded_rescan_plan`], with the indexer the wallet
/// synced from.
///
/// A rescan replays `zswapLedgerEvents` from its first event against a state
/// that starts empty and carries the wallet's registered coins (see
/// [`Wallet::watch_for_coin`]). Starting over is the point: a replay that
/// meets an output it cannot claim collapses that Merkle leaf, and a resync
/// resumes from the cursor rather than revisiting it, so a registration made
/// after the fact is only honoured by a replay that starts at zero.
///
/// The plan / run / commit split mirrors [`ResyncPlan`], and for the same
/// reason: a caller that shares the wallet across tasks snapshots under a
/// brief read lock, runs the replay with no read or write lock held, and
/// applies the result under a brief write lock.
#[must_use = "run the plan with ShieldedRescanPlan::run, then apply it with Wallet::commit_shielded_rescan"]
pub(crate) struct ShieldedRescanPlan {
    indexer: IndexerEndpoint,
    seed: Seed,
    coins: Vec<CoinInfo>,
    plan: RescanPlanKind,
}

enum RescanPlanKind {
    Ledger8(ledger_8::ShieldedRescanPlan),
    Ledger9(ledger_9::ShieldedRescanPlan),
}

impl ShieldedRescanPlan {
    /// Run the rescan's replay phase: replay the whole shielded event stream
    /// from its first event, without touching the wallet.
    ///
    /// Returns the [`ShieldedRescanCommit`] to apply with
    /// [`Wallet::commit_shielded_rescan`]. On a replay error nothing was
    /// committed anywhere, so the wallet the plan was taken from is
    /// untouched.
    ///
    /// Callers that release the wallet lock between plan and commit must
    /// serialize this against resyncs (a resync committing its own cursor and
    /// state in the middle would overwrite the rebuilt state).
    /// [`LocalWallet`](crate::LocalWallet) holds a mutex across plan → run →
    /// commit for this.
    ///
    /// A stream that starts in an earlier generation than the wallet's is
    /// replayed from that generation, and the state crosses each fork on the
    /// way.
    pub(crate) async fn run(self) -> Result<ShieldedRescanCommit, WalletError> {
        let ShieldedRescanPlan {
            indexer,
            seed,
            coins,
            plan,
        } = self;
        let indexer = &indexer;
        let result = match plan {
            RescanPlanKind::Ledger8(plan) => plan.run(indexer).await.map(RescanCommitKind::Ledger8),
            RescanPlanKind::Ledger9(plan) => plan.run(indexer).await.map(RescanCommitKind::Ledger9),
        };
        let kind = match result {
            Err(WalletError::LedgerMismatch { expected, found }) if found < expected => {
                info!(%found, "the shielded stream starts in an earlier ledger; rescanning across the fork");
                let start = ledger_8::ShieldedRescanPlan::new(&seed, &coins).initial_state;
                let sub_client = SubscriptionClient::new(indexer);
                let replayed = replay_zswap(
                    &sub_client,
                    &seed,
                    ZswapState::Ledger8(start),
                    0,
                    false,
                    None,
                )
                .await?;
                match replayed.state.into_ledger(expected)? {
                    ZswapState::Ledger8(zswap_state) => {
                        RescanCommitKind::Ledger8(ledger_8::ShieldedRescanCommit {
                            zswap_state,
                            zswap_event_id: replayed.last_id,
                        })
                    }
                    ZswapState::Ledger9(zswap_state) => {
                        RescanCommitKind::Ledger9(ledger_9::ShieldedRescanCommit {
                            zswap_state,
                            zswap_event_id: replayed.last_id,
                        })
                    }
                }
            }
            result => result?,
        };
        Ok(ShieldedRescanCommit(kind))
    }
}

/// Validated result of a shielded rescan's replay, ready to be committed into
/// a [`Wallet`] via [`Wallet::commit_shielded_rescan`].
///
/// Produced only by [`ShieldedRescanPlan::run`]; the fields are private so a
/// commit can't be forged from un-replayed state.
#[must_use = "apply with Wallet::commit_shielded_rescan, or the completed replay is discarded"]
pub(crate) struct ShieldedRescanCommit(RescanCommitKind);

enum RescanCommitKind {
    Ledger8(ledger_8::ShieldedRescanCommit),
    Ledger9(ledger_9::ShieldedRescanCommit),
}

// ---------------------------------------------------------------------------
// The sync that crosses hard forks.
// ---------------------------------------------------------------------------

/// A shielded state, in its generation.
pub(crate) enum ZswapState {
    Ledger8(midnight_helpers::ledger_8::WalletState<DefaultDB>),
    Ledger9(midnight_helpers::ledger_9::WalletState<DefaultDB>),
}

impl ZswapState {
    fn ledger_version(&self) -> LedgerVersion {
        match self {
            Self::Ledger8(_) => LedgerVersion::V8,
            Self::Ledger9(_) => LedgerVersion::V9,
        }
    }

    /// This state in the generation `target`, carried forward across the
    /// forks in between. Both generations encode the shielded state alike,
    /// and the fork kept the chain's, so the coins and the Merkle tree carry
    /// over unchanged.
    fn into_ledger(self, target: LedgerVersion) -> Result<Self, WalletError> {
        match (self, target) {
            (Self::Ledger8(state), LedgerVersion::V9) => {
                let state = midnight_helpers::fork::old_to_new_ser(&state).map_err(|e| {
                    WalletError::Sync(format!("carry the shielded state across the fork: {e}"))
                })?;
                Ok(Self::Ledger9(state))
            }
            (state, target) if state.ledger_version() == target => Ok(state),
            (state, target) => Err(WalletError::LedgerMismatch {
                expected: target,
                found: state.ledger_version(),
            }),
        }
    }
}

/// A Dust state, in its generation. A sync resumes it only on a chain of the
/// same generation: a hard fork empties the chain's Dust state.
enum DustState {
    Ledger8(midnight_helpers::ledger_8::DustWallet<DefaultDB>),
    Ledger9(midnight_helpers::ledger_9::DustWallet<DefaultDB>),
}

/// The shielded state a replay reached, and the last event it applied.
struct ZswapReplayed {
    state: ZswapState,
    last_id: i64,
}

/// Replay the zswap stream from `start_id`, starting in `state`'s
/// generation, and cross each hard fork the stream meets.
async fn replay_zswap(
    sub_client: &SubscriptionClient,
    seed: &Seed,
    state: ZswapState,
    start_id: i64,
    resuming: bool,
    progress: Option<mpsc::Sender<SyncProgress>>,
) -> Result<ZswapReplayed, WalletError> {
    let (mut state, mut start_id) = (state, start_id);
    loop {
        let (replayed, later) = match state {
            ZswapState::Ledger8(zswap) => {
                let keys = ledger_8::state::zswap_keys(seed);
                let replay = ledger_8::state::replay_zswap_events(
                    sub_client,
                    &keys,
                    zswap,
                    start_id,
                    resuming,
                    progress.clone(),
                )
                .await?;
                (
                    ZswapReplayed {
                        state: ZswapState::Ledger8(replay.state),
                        last_id: replay.last_id,
                    },
                    replay.later_ledger,
                )
            }
            ZswapState::Ledger9(zswap) => {
                let keys = ledger_9::state::zswap_keys(seed);
                let replay = ledger_9::state::replay_zswap_events(
                    sub_client,
                    &keys,
                    zswap,
                    start_id,
                    resuming,
                    progress.clone(),
                )
                .await?;
                (
                    ZswapReplayed {
                        state: ZswapState::Ledger9(replay.state),
                        last_id: replay.last_id,
                    },
                    replay.later_ledger,
                )
            }
        };
        let Some((at, found)) = later else {
            return Ok(replayed);
        };
        info!(at, %found, "the shielded stream crosses a hard fork");
        state = replayed.state.into_ledger(found)?;
        start_id = at;
    }
}

/// Where a sync resumes: a snapshot, or a wallet in memory.
struct Resume {
    zswap: ZswapState,
    /// The last shielded event the state applied.
    zswap_event_id: i64,
    dust: DustState,
    /// The last Dust event the state consumed.
    dust_event_id: i64,
    unshielded_utxos: Vec<TrackedUtxo>,
    last_tx_id: Option<i64>,
    last_block_height: i64,
}

/// Where a sync starts.
enum Start {
    /// From a snapshot, or a wallet in memory.
    Resume(Box<Resume>),
    /// From the chain's first event, with this shielded state, which is empty
    /// but for the coins it registers.
    Genesis(Box<ZswapState>),
}

/// Who a sync is for, and where it keeps what it learns.
#[derive(Clone)]
pub(crate) struct ChainSync<'a> {
    pub(crate) indexer: &'a IndexerEndpoint,
    pub(crate) seed: &'a Seed,
    pub(crate) address: &'a str,
    pub(crate) network: &'a Network,
    pub(crate) storage_dir: Option<&'a Path>,
    /// Whether the sync writes its state under `storage_dir` as it goes:
    /// Dust checkpoints, the pending reservations it loads, and the synced
    /// state. A sync whose state waits for a commit writes nothing, and the
    /// commit saves it.
    pub(crate) save: bool,
    /// The pin this sync persists, so a later resume can ask whether it is
    /// still on this chain.
    pub(crate) chain_pin: Option<ChainPin>,
}

/// One sync attempt: read the chain's generation, load the snapshot in its
/// own generation, and sync on the chain.
async fn sync_once(
    sync: ChainSync<'_>,
    progress: Option<mpsc::Sender<SyncProgress>>,
) -> Result<State, WalletError> {
    let block = latest_block(sync.indexer).await?;
    let chain = ledger_of(&block)?;
    let wallet_id = wallet_storage_id(sync.address);

    info!("loading cached state from disk");
    let snapshot = match sync.storage_dir {
        Some(dir) => load_snapshot(dir, sync.network.as_str(), &wallet_id, chain)?,
        None => None,
    };
    let (start, sync) = match snapshot {
        Some((resume, stored_pin)) => {
            info!(
                zswap_event_id = resume.zswap_event_id,
                dust_event_id = resume.dust_event_id,
                ledger = %resume.zswap.ledger_version(),
                "resuming from cached state"
            );
            let alive = send_progress(
                &progress,
                SyncProgress::Resuming {
                    zswap_event_id: resume.zswap_event_id,
                    dust_event_id: resume.dust_event_id,
                },
            );
            if !alive {
                return Err(progress_cancelled("resume"));
            }
            // Keep the snapshot's own pin when this sync has no fresher one.
            // A node that could not answer must not cost the wallet the mark
            // that lets the next resume check itself.
            let chain_pin = sync.chain_pin.or(stored_pin);
            (
                Start::Resume(Box::new(resume)),
                ChainSync { chain_pin, ..sync },
            )
        }
        None => (
            Start::Genesis(Box::new(ZswapState::Ledger8(
                midnight_helpers::ledger_8::WalletState::new(),
            ))),
            sync,
        ),
    };
    sync_on_chain(sync, &block, start, progress).await
}

/// A snapshot, in the generation that wrote it, and the chain pin it
/// carries. A snapshot of a later generation than the chain's means the
/// chain was replaced.
fn load_snapshot(
    dir: &Path,
    network_id: &str,
    wallet_id: &str,
    chain: LedgerVersion,
) -> Result<Option<(Resume, Option<ChainPin>)>, WalletError> {
    let Some(stored) = crate::storage::load_ledger_version(dir, network_id, wallet_id)? else {
        return Ok(None);
    };
    if stored > chain {
        return Err(WalletError::LedgerRegression {
            path: crate::storage::snapshot_path(dir, network_id, wallet_id)
                .display()
                .to_string(),
            snapshot: stored,
            chain,
        });
    }
    Ok(match stored {
        LedgerVersion::V8 => ledger_8::snapshot::load(dir, network_id, wallet_id)?.map(|c| {
            let resume = Resume {
                zswap: ZswapState::Ledger8(c.zswap_state),
                zswap_event_id: c.zswap_event_id,
                dust: DustState::Ledger8(c.dust_wallet),
                dust_event_id: c.dust_event_id,
                unshielded_utxos: c.unshielded_utxos,
                last_tx_id: c.last_tx_id,
                last_block_height: c.last_block_height,
            };
            (resume, c.chain_pin)
        }),
        LedgerVersion::V9 => ledger_9::snapshot::load(dir, network_id, wallet_id)?.map(|c| {
            let resume = Resume {
                zswap: ZswapState::Ledger9(c.zswap_state),
                zswap_event_id: c.zswap_event_id,
                dust: DustState::Ledger9(c.dust_wallet),
                dust_event_id: c.dust_event_id,
                unshielded_utxos: c.unshielded_utxos,
                last_tx_id: c.last_tx_id,
                last_block_height: c.last_block_height,
            };
            (resume, c.chain_pin)
        }),
    })
}

/// Sync on a chain whose latest block is `block`, from `start`. Replay the
/// shielded and unshielded streams, and cross each fork the shielded stream
/// meets. Then finish in the chain's generation.
///
/// A sync from genesis reads an indexer that sends nothing as a stall and
/// fails, since the state it would keep is the empty one it started from. A
/// resume reads the same silence as the tip.
async fn sync_on_chain(
    sync: ChainSync<'_>,
    block: &midnight_indexer_client::Block,
    start: Start,
    progress: Option<mpsc::Sender<SyncProgress>>,
) -> Result<State, WalletError> {
    let chain = ledger_of(block)?;
    let resuming = matches!(start, Start::Resume(_));
    // Where each stream starts. When resuming, start from the next event
    // after the last one applied; the subscription is inclusive, so the
    // last one itself would be delivered again. A replay from genesis
    // starts in the earliest generation this build links, and crosses
    // forward to wherever the chain's history leads.
    let (zswap_start, start_zswap_id, dust_state, start_dust_id, utxos, start_tx_id, height) =
        match start {
            Start::Resume(r) => (
                r.zswap,
                r.zswap_event_id + 1,
                Some(r.dust),
                r.dust_event_id + 1,
                r.unshielded_utxos,
                r.last_tx_id.map(|id| id + 1).unwrap_or(0),
                r.last_block_height,
            ),
            Start::Genesis(zswap) => (*zswap, 0, None, 0, Vec::new(), 0, 0),
        };

    // The fork from ledger 8 to ledger 9 emptied the chain's Dust state and
    // its registrations, so no UTXO the wallet held before the fork generates
    // Dust. The replay's events carry the indexer's current flags.
    let mut utxos = utxos;
    if matches!(dust_state, Some(DustState::Ledger8(_))) && chain == LedgerVersion::V9 {
        for utxo in &mut utxos {
            utxo.registered_for_dust_generation = Some(false);
        }
    }

    let sub_client = SubscriptionClient::new(sync.indexer);
    info!(
        start_zswap_id,
        start_tx_id, start_dust_id, "starting subscriptions"
    );
    // The first replay to fail ends the other, whose result would be
    // discarded anyway.
    let (zswap, unshielded) = tokio::try_join!(
        replay_zswap(
            &sub_client,
            sync.seed,
            zswap_start,
            start_zswap_id,
            resuming,
            progress.clone(),
        ),
        replay_unshielded_events(
            &sub_client,
            sync.address,
            utxos,
            start_tx_id,
            height,
            resuming,
            progress.clone(),
        ),
    )?;
    let UnshieldedReplay {
        utxos: unshielded_utxos,
        last_tx_id,
        last_block_height,
        spent: spent_unshielded,
    } = unshielded;

    match zswap.state.into_ledger(chain)? {
        ZswapState::Ledger8(zswap_state) => {
            let dust = match dust_state {
                Some(DustState::Ledger8(wallet)) => ledger_8::state::DustStart::Resume {
                    wallet: Box::new(wallet),
                    next_id: start_dust_id,
                },
                _ => ledger_8::state::DustStart::Fresh {
                    next_id: start_dust_id,
                },
            };
            let wallet = ledger_8::Wallet::finish_sync(
                sync,
                block,
                ledger_8::state::SyncStart {
                    zswap_state,
                    zswap_event_id: zswap.last_id,
                    unshielded_utxos,
                    last_tx_id,
                    last_block_height,
                    spent_unshielded,
                    dust,
                },
                progress,
            )
            .await?;
            Ok(State::Ledger8(wallet))
        }
        ZswapState::Ledger9(zswap_state) => {
            let dust = match dust_state {
                Some(DustState::Ledger9(wallet)) => ledger_9::state::DustStart::Resume {
                    wallet: Box::new(wallet),
                    next_id: start_dust_id,
                },
                Some(DustState::Ledger8(_)) => {
                    warn!(
                        "the chain crossed a hard fork that emptied its Dust state; Dust starts over"
                    );
                    ledger_9::state::DustStart::Fresh {
                        next_id: start_dust_id,
                    }
                }
                None => ledger_9::state::DustStart::Fresh {
                    next_id: start_dust_id,
                },
            };
            let wallet = ledger_9::Wallet::finish_sync(
                sync,
                block,
                ledger_9::state::SyncStart {
                    zswap_state,
                    zswap_event_id: zswap.last_id,
                    unshielded_utxos,
                    last_tx_id,
                    last_block_height,
                    spent_unshielded,
                    dust,
                },
                progress,
            )
            .await?;
            Ok(State::Ledger9(wallet))
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use midnight_types::hd::Seed;
    use midnight_types::{CoinInfo, HashOutput, Nonce, ShieldedTokenType};

    pub(crate) fn test_seed() -> Seed {
        Seed::from_bytes(&[0x22; 32]).unwrap()
    }

    /// A coin the wallet owns and can rebuild, but whose output carries
    /// nothing it can decrypt.
    pub(crate) fn unreachable_coin(byte: u8, value: u128) -> CoinInfo {
        CoinInfo {
            nonce: Nonce(HashOutput([byte; 32])),
            type_: ShieldedTokenType(HashOutput([3u8; 32])),
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use midnight_helpers::{Tagged, ledger_8, ledger_9};
    use midnight_indexer_client::testutil::{
        accept_subscriber, bind, ledger_event, next_json, send_next, subscription_variable,
    };

    use super::test_support::{test_seed, unreachable_coin};
    use super::*;

    /// A chain that crossed the fork serves ledger 8 events, then ledger 9
    /// ones, in one stream. A replay that stopped at the boundary, or lost the
    /// state it built before it, would lose the coins of one half. The second
    /// coin is claimed by a registration made before the fork, which the
    /// crossing must carry.
    #[tokio::test]
    async fn a_zswap_replay_crosses_the_fork_with_the_coins_of_both_halves() {
        let (listener, url) = bind().await;
        let events = [
            (
                1,
                crate::ledger_8::state::test_support::ciphertextless_output(
                    &test_seed(),
                    &unreachable_coin(1, 42),
                    0,
                ),
            ),
            (
                2,
                crate::ledger_9::state::test_support::ciphertextless_output(
                    &test_seed(),
                    &unreachable_coin(2, 42),
                    1,
                ),
            ),
        ];
        let server = tokio::spawn(async move {
            // One subscription per generation the replay passes through.
            for _ in 0..2 {
                let (mut ws, sub) = accept_subscriber(&listener).await;
                let from = subscription_variable(&sub, "id");
                for (id, raw) in events.iter().filter(|(id, _)| *id >= from) {
                    let message = ledger_event("zswapLedgerEvents", *id, raw, 2);
                    send_next(&mut ws, &sub, message).await;
                }
                while next_json(&mut ws).await.is_some() {}
            }
        });

        let registered = crate::ledger_8::state::ShieldedRescanPlan::new(
            &test_seed(),
            &[unreachable_coin(1, 42), unreachable_coin(2, 42)],
        );
        let replayed = replay_zswap(
            &SubscriptionClient::new(&url),
            &test_seed(),
            ZswapState::Ledger8(registered.initial_state),
            0,
            false,
            None,
        )
        .await
        .expect("the replay crosses the fork");
        server.await.unwrap();

        assert_eq!(replayed.last_id, 2);
        let ZswapState::Ledger9(state) = replayed.state else {
            panic!("the state must end in the generation of the last event");
        };
        assert_eq!(state.coins.iter().count(), 2, "a coin from each half");
        assert_eq!(
            state.pending_outputs.iter().count(),
            0,
            "both registrations are claimed"
        );
    }

    /// The fork emptied the chain's Dust state, and the indexer still serves
    /// the Dust events from before it. A ledger 9 replay that decoded them
    /// would fail, and one that applied them would hold Dust that no longer
    /// exists.
    #[tokio::test]
    async fn a_ledger_9_dust_replay_skips_ledger_8_events_undecoded() {
        let (listener, url) = bind().await;
        // Only the tag says ledger 8; the body would not decode.
        let raw = hex::encode(format!(
            "midnight:{}:garbage",
            ledger_8::Event::<DefaultDB>::tag()
        ));
        let server = tokio::spawn(async move {
            let (mut ws, sub) = accept_subscriber(&listener).await;
            for id in 1..=3 {
                let message = ledger_event("dustLedgerEvents", id, &raw, 3);
                send_next(&mut ws, &sub, message).await;
            }
            while next_json(&mut ws).await.is_some() {}
        });

        let wallet = ledger_9::DustWallet::default(
            midnight_types::ledger_9::convert::IntoLedger::into_ledger(&test_seed()),
            Some(&ledger_9::INITIAL_PARAMETERS),
        );
        let replay = crate::ledger_9::state::replay_dust_events(
            &SubscriptionClient::new(&url),
            wallet,
            0,
            false,
            None::<fn(&ledger_9::DustWallet<DefaultDB>, i64)>,
            None,
        )
        .await
        .expect("a ledger 9 replay skips ledger 8 events");
        server.await.unwrap();

        assert_eq!(
            replay.last_id, 3,
            "the skipped events still move the cursor"
        );
        assert!(replay.later_ledger.is_none());
        assert!(replay.spend_nullifiers.is_empty());
    }

    /// Mock-indexer tests of a rebuild from genesis. The mock serves the
    /// latest block over HTTP and the subscriptions over WebSocket, on one
    /// port.
    mod genesis_rebuild_ws {
        use midnight_helpers::ledger_9::midnight_serialize::tagged_serialize;
        use midnight_indexer_client::testutil::{
            read_http_request, send_json, subscriber_handshake, unshielded_progress,
            write_json_response,
        };
        use serde_json::json;
        use tokio::net::TcpStream;

        use super::*;

        /// What the mock sends on a subscription: the parsed `subscribe`
        /// message in, the `data` of each `next` message out.
        type Stream = fn(&serde_json::Value) -> Vec<serde_json::Value>;

        /// The latest block of a ledger 9 chain.
        fn latest_block_body() -> String {
            let mut parameters = Vec::new();
            tagged_serialize(&ledger_9::INITIAL_PARAMETERS, &mut parameters).unwrap();
            json!({"data": {"block": {
                "hash": "00".repeat(32),
                "height": 1,
                "timestamp": 1_000_000,
                "ledgerParameters": hex::encode(parameters),
            }}})
            .to_string()
        }

        /// An indexer that answers the latest-block query and every
        /// subscription, sending on each what `stream` returns for it and
        /// then nothing more. It answers the protocol's keepalive pings, so
        /// it is alive however long it stays silent.
        async fn indexer(stream: Stream) -> IndexerEndpoint {
            let (listener, url) = midnight_indexer_client::testutil::bind().await;
            tokio::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    tokio::spawn(serve(socket, stream));
                }
            });
            IndexerEndpoint::from(url)
        }

        async fn serve(mut socket: TcpStream, stream: Stream) {
            let mut head = [0u8; 2048];
            let n = socket.peek(&mut head).await.unwrap();
            let head = String::from_utf8_lossy(&head[..n]).to_ascii_lowercase();
            if !head.contains("upgrade: websocket") {
                if read_http_request(&mut socket).await {
                    write_json_response(&mut socket, "200 OK", &latest_block_body()).await;
                }
                return;
            }
            let (mut ws, sub) = subscriber_handshake(socket).await;
            for data in stream(&sub) {
                let id = sub["id"].as_str().unwrap();
                let message = json!({"type": "next", "id": id, "payload": {"data": data}});
                send_json(&mut ws, &message).await;
            }
            while let Some(message) = next_json(&mut ws).await {
                if message["type"] == "ping" {
                    send_json(&mut ws, &json!({"type": "pong"})).await;
                }
            }
        }

        fn silent(_: &serde_json::Value) -> Vec<serde_json::Value> {
            Vec::new()
        }

        fn wallet(indexer: IndexerEndpoint) -> Wallet {
            Wallet {
                seed: test_seed(),
                state: State::Ledger9(crate::ledger_9::state::test_support::offline_wallet(
                    None, indexer,
                )),
            }
        }

        /// The state a rebuild starts from is empty, so an indexer with
        /// nothing to say must fail it. Read as the tip, the silence commits
        /// that empty state and wipes the wallet.
        #[tokio::test]
        async fn a_rebuild_reads_a_silent_indexer_as_a_stall_not_the_tip() {
            let wallet = wallet(indexer(silent).await);

            let result = wallet.resync_plan_from_genesis().run(None).await;

            match result {
                Err(err @ WalletError::Indexer { .. }) => assert!(
                    err.to_string().contains("timeout waiting"),
                    "expected the rebuild to give up on the silence, got: {err}"
                ),
                Err(other) => panic!("expected a stall, got: {other:?}"),
                Ok(_) => panic!(
                    "a rebuild that reads silence as the tip commits the empty state it started from"
                ),
            }
        }

        #[tokio::test]
        async fn a_resync_from_cursors_reads_a_silent_indexer_as_the_tip() {
            let mut wallet = wallet(indexer(silent).await);
            let before = wallet.sync_cursors();

            let commit = wallet
                .resync_plan()
                .run(None)
                .await
                .expect("a resume with nothing new is at the tip");
            wallet.commit_resync(commit).unwrap();

            let after = wallet.sync_cursors();
            assert_eq!(after.zswap_event_id, before.zswap_event_id);
            assert_eq!(after.dust_event_id, before.dust_event_id);
        }

        /// A chain whose shielded history is one output to the wallet's
        /// registered coin, and which has no Dust and no unshielded
        /// transactions for it. The output is a ledger 9 event, so a replay
        /// that starts in ledger 8 crosses on its first event.
        fn one_registered_output(sub: &serde_json::Value) -> Vec<serde_json::Value> {
            let query = sub["payload"]["query"].as_str().unwrap();
            if query.contains("zswapLedgerEvents") {
                let raw = crate::ledger_9::state::test_support::ciphertextless_output(
                    &test_seed(),
                    &unreachable_coin(5, 42),
                    0,
                );
                vec![ledger_event("zswapLedgerEvents", 1, &raw, 1)]
            } else if query.contains("dustLedgerEvents") {
                vec![ledger_event("dustLedgerEvents", 0, "00", 0)]
            } else {
                vec![unshielded_progress(0)]
            }
        }

        /// A rebuild replays from the chain's first event, whatever the
        /// cursors say, claims the coins the wallet registered on the way, and
        /// reports its progress as a first sync does.
        ///
        /// The wallet's zswap cursor is past the chain's only event, so a
        /// replay from the cursors skips it, claims nothing, and keeps the
        /// cursor.
        #[tokio::test]
        async fn a_rebuild_replays_from_the_first_event_and_reports_it() {
            let mut wallet = wallet(indexer(one_registered_output).await);
            wallet
                .commit_shielded_rescan(ShieldedRescanCommit(RescanCommitKind::Ledger9(
                    crate::ledger_9::ShieldedRescanCommit {
                        zswap_state: ledger_9::WalletState::new(),
                        zswap_event_id: 50,
                    },
                )))
                .unwrap();
            wallet.watch_for_coin(unreachable_coin(5, 42)).unwrap();

            let (tx, mut rx) = mpsc::channel(64);
            let commit = wallet
                .resync_plan_from_genesis()
                .run(Some(tx))
                .await
                .expect("the rebuild replays the chain");
            wallet.commit_resync(commit).unwrap();

            assert_eq!(wallet.sync_cursors().zswap_event_id, 1);
            let coins = wallet.spendable_shielded_coins();
            assert_eq!(coins.len(), 1, "the registered coin must be claimed");
            assert_eq!(coins[0].value, 42);
            assert_eq!(wallet.ledger_version(), LedgerVersion::V9);
            let mut reported = Vec::new();
            while let Ok(update) = rx.try_recv() {
                reported.push(format!("{update:?}"));
            }
            assert!(
                reported.iter().any(|u| u.starts_with("ZswapComplete")),
                "the rebuild must report its replay: {reported:?}"
            );
        }

        /// A node whose chain holds the block `0xbb…` at every height, so a
        /// pin to any other block reads as a replaced chain.
        struct ReplacedChain;

        #[async_trait::async_trait]
        impl midnight_types::chain_pin::ChainView for ReplacedChain {
            async fn block_hashes_at(
                &self,
                _height: u64,
            ) -> Option<Vec<midnight_types::BlockHash>> {
                Some(vec![midnight_types::BlockHash::repeat_byte(0xbb)])
            }

            async fn finalized_height(&self) -> Option<u64> {
                Some(9)
            }
        }

        /// A replaced chain ends a resync from the cursors, which count the
        /// events of the chain that is gone. A rebuild from genesis is what
        /// recovers from it, so it must proceed, and leave the wallet pinned
        /// to the chain it replayed.
        #[tokio::test]
        async fn a_rebuild_proceeds_on_a_replaced_chain_that_a_resync_refuses() {
            use crate::{LocalWallet, WalletFacade};

            let mut wallet = wallet(indexer(one_registered_output).await);
            wallet.set_chain_pin(ChainPin {
                height: 4,
                hash: format!("{:#x}", midnight_types::BlockHash::repeat_byte(0xaa)),
            });
            let wallet = LocalWallet::new(wallet);

            match wallet.resync(&ReplacedChain).await {
                Err(WalletError::ChainMismatch { .. }) => {}
                other => {
                    panic!("a resync from the cursors must refuse a replaced chain, got {other:?}")
                }
            }
            wallet
                .resync_from_genesis(&ReplacedChain, None)
                .await
                .expect("a rebuild replays the chain the node runs now");

            let pin = wallet.inner().read().await.chain_pin().cloned();
            assert_eq!(
                pin,
                Some(ChainPin {
                    height: 9,
                    hash: format!("{:#x}", midnight_types::BlockHash::repeat_byte(0xbb)),
                })
            );
            wallet
                .resync(&ReplacedChain)
                .await
                .expect("the rebuilt wallet is on the chain the node runs");
        }
    }

    /// Pointing a stored ledger 9 wallet at a ledger 8 chain, such as a
    /// devnet recreated on the other generation, must say which directory to
    /// remove, as the chain pin's error does.
    #[test]
    fn a_snapshot_of_a_later_generation_names_the_directory_to_remove() {
        let base = tempfile::TempDir::new().unwrap();
        let dir = crate::storage::snapshot_path(base.path(), "undeployed", "wallet");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("metadata.json"),
            r#"{
                "generation": 1,
                "ledger_version": "V9",
                "zswap_event_id": 1,
                "dust_event_id": 1,
                "last_block_height": 0,
                "last_tx_id": null,
                "unshielded_utxos": []
            }"#,
        )
        .unwrap();

        let Err(err) = load_snapshot(base.path(), "undeployed", "wallet", LedgerVersion::V8) else {
            panic!("a ledger 9 snapshot must not load for a ledger 8 chain");
        };
        let message = err.to_string();
        assert!(
            matches!(err, WalletError::LedgerRegression { .. }),
            "{message}"
        );
        assert!(
            message.contains(&dir.display().to_string()),
            "the error must name what to remove: {message}"
        );
    }
}
