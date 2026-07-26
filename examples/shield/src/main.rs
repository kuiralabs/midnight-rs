//! Spike (#105): shield unshielded (transparent) NIGHT into native **shielded**
//! NIGHT on a local devnet — proves the ledger balances a cross-segment shield
//! (unshielded input → shielded output) in one transaction.
//!
//! Setup: fund the wallet's UNSHIELDED address with NIGHT + register dust first.
//!   MIDNIGHT_NODE_URL=ws://127.0.0.1:9944 MIDNIGHT_INDEXER_URL=http://127.0.0.1:8088 \
//!     cargo run -p example-shield

use std::env;

use midnight_provider::{MidnightProvider, Network, Seed};
use tracing_subscriber::EnvFilter;

/// Hardcoded dev seed (local devnet). Override with MIDNIGHT_WALLET_SEED.
const DEV_SEED: &str = "0000000000000000000000000000000000000000000000000000000000000001";

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| {
        eprintln!("error: {name} environment variable is required");
        std::process::exit(1);
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("midnight_wallet=info".parse()?)
                .add_directive("midnight_indexer_client=info".parse()?),
        )
        .with_target(true)
        .init();

    let node_url = required_env("MIDNIGHT_NODE_URL");
    let indexer_url = required_env("MIDNIGHT_INDEXER_URL");
    let network: Network = env::var("MIDNIGHT_NETWORK")
        .unwrap_or_else(|_| "undeployed".into())
        .into();
    let seed_hex = env::var("MIDNIGHT_WALLET_SEED").unwrap_or_else(|_| DEV_SEED.into());
    let seed = Seed::from_hex(&seed_hex)?;
    let amount: u128 = env::var("SHIELD_AMOUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    println!("=== Shield spike: unshielded NIGHT -> native shielded NIGHT ===\n");
    println!("Network:          {network}");
    println!("Unshielded addr:  {}", seed.unshielded_address(&network));
    println!("Shielded addr:    {}", seed.shielded_address(&network));
    println!("Shield amount:    {amount}\n");

    println!("Syncing wallet...");
    let provider = MidnightProvider::new(&node_url, &indexer_url)?
        .sync_wallet(seed.clone(), &network)
        .await?;
    println!("Sync complete.\n");

    let pre = provider.balance().await?;
    let night_total: u128 = pre.unshielded.iter().filter(|u| u.value > 0).map(|u| u.value).sum();
    println!("--- Pre-shield ---");
    println!("unshielded NIGHT total: {night_total}");
    println!(
        "dust: {} SPECK, spendable UTXOs: {}",
        pre.dust.balance_speck, pre.dust.spendable_utxos
    );
    println!("shielded coins: {}", pre.shielded.coins.len());
    for c in &pre.shielded.coins {
        println!("  {c}");
    }
    println!();

    if night_total < amount {
        return Err(format!(
            "insufficient unshielded NIGHT: have {night_total}, need {amount}.\n\
             Airdrop: mn airdrop <amount> --wallet {}",
            seed.unshielded_address(&network)
        )
        .into());
    }
    if pre.dust.spendable_utxos == 0 {
        return Err(
            "no dust to pay the fee — register dust first (provider.register_dust / mn dust)".into(),
        );
    }

    let recipient = seed.shielded_address(&network);
    println!("Shielding {amount} NIGHT -> own shielded address...\n");

    let pending = provider.shield(amount, &recipient).await?;
    println!("Submitted: ext hash {}", pending.extrinsic_hash_hex());
    let (best, pending) = pending.wait_best().await?;
    println!("Best:      {}", hex::encode(best.block_hash));
    let (finalized, _) = pending.wait_finalized().await?;
    println!("Finalized: {}\n", hex::encode(finalized.block_hash));

    println!("Resyncing...");
    provider.resync_wallet().await?;
    let post = provider.balance().await?;
    let post_night: u128 = post.unshielded.iter().filter(|u| u.value > 0).map(|u| u.value).sum();
    println!("\n--- Post-shield shielded coins ---");
    for c in &post.shielded.coins {
        println!("  {c}");
    }
    println!("\nunshielded NIGHT total: {post_night} (was {night_total})");
    println!("dust: {} SPECK", post.dust.balance_speck);
    println!(
        "\n=== Done — expect a NEW shielded coin of token type 0000…0000 (native shielded NIGHT) ==="
    );
    Ok(())
}
