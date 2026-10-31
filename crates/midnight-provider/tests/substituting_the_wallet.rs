//! The provider serves a wallet it did not write.
//!
//! It holds `Arc<dyn WalletFacade>`, so a reading it answers comes from
//! whatever implements that trait. This file implements one that is not a
//! `Wallet` and attaches it.
//!
//! The test is mostly its own compilation. Narrowing `with_wallet` back to a
//! concrete type, or adding a trait method only `Wallet` can satisfy (one
//! handing out a `&Wallet`, or a lock guard), breaks this file and nothing
//! else.
//!
//! No devnet: nothing here reaches the network.

use std::sync::{Arc, Mutex};

use midnight_helpers::{
    CoinInfo, CoinPublicKey, DefaultDB, EncryptionPublicKey, LedgerContext, LedgerParameters,
    ProofProvider, WalletSeed,
};
use midnight_provider::{
    MidnightProvider, Network, ReservedBuild, SpendableShieldedCoin, SpentInputs, SyncCursors,
    TrackedUtxo, TransferRequest, WalletBalance, WalletError, WalletFacade,
};
use midnight_wallet::chain_pin::ChainView;

/// A wallet that answers the three readings this test makes and refuses the
/// rest. Everything it refuses would need chain state to answer honestly.
/// It records which resync it was asked for, since the two do the same thing
/// here and differ only in which one the provider reached.
#[derive(Default)]
struct StubWallet {
    resyncs: Arc<Mutex<Vec<&'static str>>>,
}

#[async_trait::async_trait]
impl WalletFacade for StubWallet {
    async fn network(&self) -> Network {
        Network::Preprod
    }

    async fn sync_cursors(&self) -> SyncCursors {
        SyncCursors {
            last_block_height: 12,
            last_tx_id: Some(34),
            zswap_event_id: 56,
            dust_event_id: 78,
        }
    }

    async fn dust_synced(&self) -> bool {
        true
    }

    async fn seed(&self) -> WalletSeed {
        unimplemented!("this wallet holds no seed")
    }

    async fn shielded_public_keys(&self) -> (CoinPublicKey, EncryptionPublicKey) {
        unimplemented!("this wallet holds no keys")
    }

    async fn balance(&self) -> WalletBalance {
        unimplemented!("this wallet holds no coins")
    }

    async fn spendable_shielded_coins(&self) -> Vec<SpendableShieldedCoin> {
        unimplemented!("this wallet holds no coins")
    }

    async fn unshielded_utxos(&self) -> Vec<TrackedUtxo> {
        Vec::new()
    }

    async fn parameters(&self) -> LedgerParameters {
        unimplemented!("this wallet has synced no parameters")
    }

    async fn execution_context(&self) -> Result<Arc<LedgerContext<DefaultDB>>, WalletError> {
        Err(WalletError::Sync("stub wallet has no chain state".into()))
    }

    async fn add_funding(&self, _context: &LedgerContext<DefaultDB>) -> Result<(), WalletError> {
        Err(WalletError::Sync("stub wallet funds nothing".into()))
    }

    async fn prepare_transfer(
        &self,
        _request: TransferRequest,
        _proof_provider: Arc<dyn ProofProvider<DefaultDB>>,
    ) -> Result<ReservedBuild, WalletError> {
        Err(WalletError::Transfer("stub wallet builds nothing".into()))
    }

    async fn prepare_funded(
        &self,
        _tx_info: midnight_helpers::StandardTrasactionInfo<DefaultDB>,
    ) -> Result<ReservedBuild, WalletError> {
        Err(WalletError::Transfer("stub wallet funds nothing".into()))
    }

    async fn spend_shielded(
        &self,
        _context: &Arc<LedgerContext<DefaultDB>>,
        _nullifiers: Vec<midnight_helpers::Nullifier>,
        _rng: &mut midnight_helpers::StdRng,
    ) -> Result<(Vec<midnight_types::PreparedInput>, SpentInputs), WalletError> {
        unimplemented!("this wallet holds no coins")
    }

    async fn prepare_fees(
        &self,
        _tx_info: midnight_helpers::StandardTrasactionInfo<DefaultDB>,
        _external: &midnight_helpers::FinalizedTransaction<DefaultDB>,
    ) -> Result<Option<ReservedBuild>, WalletError> {
        Err(WalletError::Transfer("stub wallet funds nothing".into()))
    }

    async fn release(&self, _spent: &SpentInputs) {}

    async fn resync(&self, _chain: &dyn ChainView) -> Result<(), WalletError> {
        self.resyncs.lock().unwrap().push("delta");
        Ok(())
    }

    async fn resync_from_genesis(&self, _chain: &dyn ChainView) -> Result<(), WalletError> {
        self.resyncs.lock().unwrap().push("genesis");
        Ok(())
    }

    async fn rescan_shielded(&self) -> Result<(), WalletError> {
        Ok(())
    }

    async fn watch_for_coins(&self, _coins: Vec<CoinInfo>) -> Result<(), WalletError> {
        Ok(())
    }

    async fn forget_coins(&self, _coins: Vec<CoinInfo>) -> Result<(), WalletError> {
        Ok(())
    }
}

#[tokio::test]
async fn the_provider_reads_whatever_wallet_it_was_given() {
    let provider = MidnightProvider::new("ws://test", "http://test")
        .expect("provider")
        .with_wallet(StubWallet::default());

    assert_eq!(
        provider.network().await.expect("attached"),
        Network::Preprod
    );
    assert!(provider.dust_synced().await.expect("attached"));
    assert_eq!(
        provider
            .sync_cursors()
            .await
            .expect("attached")
            .zswap_event_id,
        56
    );
}

#[tokio::test]
async fn a_genesis_resync_reaches_the_wallets_genesis_rebuild() {
    let wallet = StubWallet::default();
    let resyncs = wallet.resyncs.clone();
    let provider = MidnightProvider::new("ws://test", "http://test")
        .expect("provider")
        .with_wallet(wallet);

    provider
        .resync_wallet_from_genesis()
        .await
        .expect("the stub resyncs without a chain");

    assert_eq!(
        *resyncs.lock().unwrap(),
        vec!["genesis"],
        "a genesis resync that lands on the delta resync replays nothing and leaves the state it was asked to rebuild"
    );
}
