//! Reading a shielded swap offer from its bytes alone.
//!
//! An offer is one wallet's half of a shielded swap: a sealed ledger 9
//! transaction whose only part is a shielded offer in the guaranteed segment.
//! It spends what its maker gives and pays its maker what it wants back, so it
//! is deliberately unbalanced, and the ledger takes it only merged with the
//! mirrored half. Nothing said about an offer is trusted: what it gives, what
//! it wants and which coins it spends come from the bytes.

use midnight_helpers::DefaultDB;
use midnight_helpers::ledger_9::midnight_serialize::tagged_deserialize;
use midnight_helpers::ledger_9::{FinalizedTransaction, Transaction};
use sha2::{Digest, Sha256};

use crate::{HashOutput, Nullifier, ShieldedTokenType};

/// One side of a swap: a shielded token and how much of it, in base units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapLeg {
    pub token: ShieldedTokenType,
    pub amount: u128,
}

/// What an offer's bytes say it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwapOfferContents {
    /// The SHA-256 of the offer's bytes, the id makers and takers name it by.
    pub id: [u8; 32],
    /// What the maker gives.
    pub give: SwapLeg,
    /// What the maker wants back.
    pub want: SwapLeg,
    /// The nullifiers of the coins the offer spends. Once any of them is on
    /// chain, the offer was taken or cancelled and can no longer be taken.
    pub nullifiers: Vec<Nullifier>,
}

/// Bytes that are not a shielded swap offer, and why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("not a swap offer: {0}")]
pub struct NotASwapOffer(pub String);

/// Read a swap offer from its bytes: a sealed ledger 9 transaction whose only
/// part is a shielded offer in the guaranteed segment, giving one token for one
/// other.
///
/// # Errors
///
/// [`NotASwapOffer`] for bytes that are not a sealed ledger 9 transaction, a
/// transaction with intents or fallible shielded parts, or an offer that does
/// not give exactly one token for exactly one other.
pub fn read_swap_offer(bytes: &[u8]) -> Result<SwapOfferContents, NotASwapOffer> {
    let tx: FinalizedTransaction<DefaultDB> = tagged_deserialize(&mut &bytes[..])
        .map_err(|e| NotASwapOffer(format!("it is not a sealed ledger 9 transaction ({e})")))?;
    let Transaction::Standard(standard) = tx else {
        return Err(NotASwapOffer("it is a rewards claim".into()));
    };
    let offer = match &standard.guaranteed_coins {
        Some(offer) if standard.intents.size() == 0 && standard.fallible_coins.size() == 0 => offer,
        _ => {
            return Err(NotASwapOffer(
                "a swap offer has shielded parts only, in the guaranteed segment".into(),
            ));
        }
    };
    // A delta is inputs less outputs: what the maker gives is in surplus, what it wants in deficit.
    let deltas: Vec<_> = offer
        .deltas
        .iter()
        .map(|d| (d.token_type, d.value))
        .collect();
    let gives: Vec<_> = deltas.iter().filter(|(_, v)| *v > 0).collect();
    let wants: Vec<_> = deltas.iter().filter(|(_, v)| *v < 0).collect();
    let ([(give_token, give)], [(want_token, want)]) = (gives.as_slice(), wants.as_slice()) else {
        return Err(NotASwapOffer(format!(
            "a swap offer gives one token for one other, not {} for {}",
            gives.len(),
            wants.len()
        )));
    };
    Ok(SwapOfferContents {
        id: Sha256::digest(bytes).into(),
        give: SwapLeg {
            token: ShieldedTokenType(give_token.0),
            amount: give.unsigned_abs(),
        },
        want: SwapLeg {
            token: ShieldedTokenType(want_token.0),
            amount: want.unsigned_abs(),
        },
        nullifiers: offer
            .inputs
            .iter()
            .map(|input| Nullifier(HashOutput(input.nullifier.0.0)))
            .collect(),
    })
}
