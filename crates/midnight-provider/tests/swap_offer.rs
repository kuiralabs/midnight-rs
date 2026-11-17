//! Reading swap offers from real bytes: an offer mn 0.6.1 made on a ledger 9
//! localnet, and a NIGHT transfer the same chain finalized.

use midnight_provider::{HashOutput, Nullifier, ShieldedTokenType, SwapLeg, read_swap_offer};

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    hex::decode(std::fs::read_to_string(&path).unwrap().trim()).unwrap()
}

fn token(last: u8) -> ShieldedTokenType {
    let mut bytes = [0; 32];
    bytes[31] = last;
    ShieldedTokenType(HashOutput(bytes))
}

fn hash(hex: &str) -> [u8; 32] {
    hex::decode(hex).unwrap().try_into().unwrap()
}

/// The terms, id and coins are the ones mn's own reader gives for the same bytes:
/// 10 of token 1 for 7 of token 2, one coin spent.
#[test]
fn an_offer_mn_made_reads_as_mn_reads_it() {
    let offer = read_swap_offer(&fixture("swap-offer-mn.hex")).unwrap();

    assert_eq!(
        offer.id,
        hash("ab92a264e4d5a3fb29518a97c6ed5a2fbb6abc40a8f5e81d030381407e470ea1")
    );
    assert_eq!(
        offer.give,
        SwapLeg {
            token: token(1),
            amount: 10
        }
    );
    assert_eq!(
        offer.want,
        SwapLeg {
            token: token(2),
            amount: 7
        }
    );
    assert_eq!(
        offer.nullifiers,
        vec![Nullifier(HashOutput(hash(
            "d213855ca57a37f6f4e88db14b06e4dc3fdcfe214f4174c95d8089e524fc45e1"
        )))]
    );
}

/// A finalized transfer is a sealed transaction too, but it has intents: it is not an offer.
#[test]
fn a_transfer_is_not_an_offer() {
    let err = read_swap_offer(&fixture("night-transfer.hex")).unwrap_err();

    assert!(err.0.contains("guaranteed segment"), "{err}");
}

/// Cut or altered bytes are not a transaction at all.
#[test]
fn bytes_that_are_not_a_transaction_are_refused() {
    let offer = fixture("swap-offer-mn.hex");

    for bytes in [
        &offer[..offer.len() - 1],
        &offer[..40],
        &[][..],
        b"not a transaction",
    ] {
        let err = read_swap_offer(bytes).unwrap_err();
        assert!(err.0.contains("not a sealed ledger 9 transaction"), "{err}");
    }
}

/// Bytes after the transaction would change the id without changing the offer, so they are refused, as mn's
/// reader refuses them.
#[test]
fn bytes_after_the_transaction_are_refused() {
    let mut offer = fixture("swap-offer-mn.hex");
    offer.push(0);

    assert!(read_swap_offer(&offer).is_err());
}
