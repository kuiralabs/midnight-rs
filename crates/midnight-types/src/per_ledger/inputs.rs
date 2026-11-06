//! Shielded inputs produced by the wallet, not by the offer builder.
//!
//! Spending a coin proves ownership, so it needs the spending keys and no
//! public-material substitute exists. What can change is where those keys are
//! read. The helpers' `InputInfo` carries a seed into the offer and resolves it
//! during `build`, so the builder holds a secret for the lifetime of the
//! transaction. Here the wallet performs the spend up front and the builder
//! receives a finished `Input`, which carries none.
//!
//! That is also the shape an external signer needs: "give me an `Input` for this
//! coin" is one step it can implement without releasing anything.

use std::sync::Arc;

use super::helpers;
use helpers::{
    BuildContext, DefaultDB, Input, Nullifier, ProofPreimage, Segment, ShieldedTokenType, StdRng,
    TokenInfo,
};

use super::convert::IntoLedger;
use crate::WalletError;
use crate::hd::Seed;

/// A shielded input the wallet already produced.
///
/// `build` hands it over and ignores the rng and context it is passed: the
/// spend has happened, and repeating it would produce a different nullifier for
/// a coin already committed to this transaction.
pub struct PreparedInput {
    input: Option<Input<ProofPreimage, DefaultDB>>,
    token_type: ShieldedTokenType,
    value: u128,
}

impl TokenInfo for PreparedInput {
    fn token_type(&self) -> ShieldedTokenType {
        self.token_type
    }

    /// The value the spent coin carries, read from the wallet and fixed before
    /// the offer sums it.
    ///
    /// The offer's declared delta is built from this, and the ledger checks
    /// that delta against the commitments the spend proofs carry, so a figure
    /// that did not come from the coin would make the transaction malformed.
    /// The helpers' input instead rewrites this during `build` with whichever
    /// coin it resolved, which is how a request for less than a coin's value
    /// used to enter the offer as the whole coin.
    fn value(&self) -> u128 {
        self.value
    }
}

impl helpers::BuildInput<DefaultDB, BuildContext> for PreparedInput {
    fn build(
        &mut self,
        _rng: &mut StdRng,
        _context: Arc<BuildContext>,
    ) -> Input<ProofPreimage, DefaultDB> {
        self.input
            .take()
            .expect("a prepared input is built once; the offer builder consumes each input once")
    }
}

/// Spend the named coins from `seed`'s wallet, returning inputs the offer
/// builder can hold without a secret.
///
/// A coin is named only by its nullifier. Its token and value come from the
/// wallet, never from the caller, so the figure the offer sums is the one the
/// spend proof commits to and the two cannot drift apart. The wallet's Zswap
/// state is rolled forward as it goes, the same bookkeeping the helpers' input
/// does inside `build`.
///
/// A spend leaves the coin in the wallet's Zswap state, so a second spend of
/// it succeeds. The function refuses a coin that `nullifiers` names twice,
/// before it spends anything.
///
/// # Errors
///
/// [`WalletError::DuplicateInput`] when `nullifiers` names a coin twice, and
/// [`WalletError::Transfer`] when the wallet cannot spend a coin.
///
/// # Panics
///
/// Panics when `context` holds no wallet for `seed`.
pub fn prepare_shielded_inputs(
    context: &Arc<BuildContext>,
    seed: &Seed,
    nullifiers: &[Nullifier],
    rng: &mut StdRng,
) -> Result<Vec<PreparedInput>, WalletError> {
    crate::refuse_repeated_nullifiers(nullifiers)?;
    context.with_wallet_from_seed(seed.into_ledger(), |wallet| {
        let mut prepared = Vec::with_capacity(nullifiers.len());
        for nullifier in nullifiers {
            let coin = wallet.shielded.state.coins.get(nullifier).ok_or_else(|| {
                // Deliberately not naming the wallet: the upstream selector
                // panics here with the whole state in the message.
                WalletError::Transfer(format!(
                    "shielded coin {nullifier:?} is not spendable by this wallet \
                         (already spent, not yet synced, or not owned by it)"
                ))
            })?;
            // Read before the spend, which replaces the state this borrows.
            let (token_type, value) = (coin.type_, coin.value);

            let (updated, input) = wallet
                .shielded
                .state
                .spend(
                    rng,
                    wallet.shielded.secret_keys(),
                    coin,
                    Segment::Guaranteed.into(),
                )
                .map_err(|e| WalletError::Transfer(format!("spend shielded coin: {e:?}")))?;
            wallet.shielded.state = updated;

            prepared.push(PreparedInput {
                input: Some(input),
                token_type,
                value,
            });
        }
        Ok(prepared)
    })
}

/// An unshielded UTXO the wallet selected for a builder to spend.
///
/// Unlike a shielded spend, an unshielded one cannot be finished up front: its
/// signature covers the whole intent, which the builder assembles later. So the
/// builder gets the spend and the one key that signs it, not the seed that
/// derives every key of the wallet.
pub struct PreparedUtxoSpend {
    spend: helpers::UtxoSpend,
    key: helpers::TransactionSigningKey,
}

#[async_trait::async_trait]
impl helpers::BuildUtxoSpend<DefaultDB, BuildContext> for PreparedUtxoSpend {
    async fn build(&self, _context: Arc<BuildContext>) -> helpers::UtxoSpend {
        self.spend.clone()
    }

    fn signing_key(&self, _context: Arc<BuildContext>) -> helpers::TransactionSigningKey {
        self.key.clone()
    }
}

/// The unshielded value a wallet put toward a build: the spends, the change
/// back to the wallet, and the UTXOs to reserve.
pub struct PreparedUnshielded {
    /// One spend per selected UTXO.
    pub inputs: Vec<PreparedUtxoSpend>,
    /// What the inputs carry beyond `amount`, paid back to the wallet's own
    /// address, or `None` when they carry exactly `amount`.
    pub change: Option<helpers::UtxoOutputInfo<helpers::UnshieldedWallet>>,
    /// The UTXOs the inputs spend, which the wallet reserves.
    pub spent: Vec<crate::SpentUtxoKey>,
}

/// Select UTXOs of `token_type` that `seed` owns, covering `amount`, from the
/// funding view in `context`.
///
/// # Errors
///
/// [`WalletError::InsufficientUnshielded`] when the view holds less than
/// `amount` of the token.
pub async fn prepare_unshielded_inputs(
    context: &Arc<BuildContext>,
    seed: &Seed,
    token_type: helpers::UnshieldedTokenType,
    amount: u128,
    strategy: helpers::CoinSelectionStrategy,
) -> Result<PreparedUnshielded, WalletError> {
    let owner = seed.into_ledger();
    let (selected, change) = match helpers::UtxoSpendInfo::utxos_to_cover_value(
        context.clone(),
        owner.clone(),
        amount,
        token_type,
        strategy,
    )
    .await
    {
        Ok(selected) => selected,
        Err(e) => return Err(super::builder::utxo_selection_error(context, e).await),
    };
    let wallet = helpers::UnshieldedWallet::default(owner);
    let key = wallet.transaction_signing_key();
    let mut inputs = Vec::with_capacity(selected.len());
    let mut spent = Vec::with_capacity(selected.len());
    for info in &selected {
        let spend =
            helpers::BuildUtxoSpend::<DefaultDB, BuildContext>::build(info, context.clone()).await;
        spent.push(crate::SpentUtxoKey {
            intent_hash: spend.intent_hash.0,
            output_index: spend.output_no,
        });
        inputs.push(PreparedUtxoSpend {
            spend,
            key: key.clone(),
        });
    }
    let change = (change > 0).then(|| helpers::UtxoOutputInfo {
        value: change,
        owner: helpers::UnshieldedWallet::from(wallet.user_address),
        token_type,
    });
    Ok(PreparedUnshielded {
        inputs,
        change,
        spent,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use helpers::{HashOutput, SeedableRng};

    /// A spend leaves the coin spendable in the wallet's state, so a coin
    /// named twice would be spent twice and its value counted twice. The
    /// wallet here holds no coin, so a check that ran only after the first
    /// spend would return the "not spendable" error instead.
    #[test]
    fn a_coin_named_twice_is_refused_before_any_spend() {
        let seed = Seed::from_bytes(&[1; 32]).unwrap();
        let context = Arc::new(BuildContext::new_from_wallet_seeds(
            "undeployed",
            &[seed.into_ledger()],
        ));
        let coin = Nullifier(HashOutput([7; 32]));
        let other = Nullifier(HashOutput([8; 32]));

        let err = prepare_shielded_inputs(
            &context,
            &seed,
            &[coin, other, coin],
            &mut StdRng::seed_from_u64(0),
        )
        .err();
        assert!(
            matches!(err, Some(WalletError::DuplicateInput { .. })),
            "got {err:?}"
        );
    }
}
