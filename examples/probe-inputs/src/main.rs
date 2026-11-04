//! Scratch probe: register a wallet's NIGHT for Dust, then send one coin's worth.
use std::env;
use std::time::Duration;
use midnight_core::wallet::NIGHT;
use midnight_core::{MidnightProvider, Network, Seed, WalletBuilder};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    let node = env::var("MIDNIGHT_NODE_URL")?;
    let indexer = env::var("MIDNIGHT_INDEXER_URL")?;
    let seed: Seed = std::fs::read_to_string(env::var("PROBE_SEED_FILE")?)?.trim().parse()?;
    let amount: u128 = env::var("PROBE_AMOUNT")?.parse()?;
    let network: Network = env::var("PROBE_NETWORK")?.parse()?;
    let recipient = Seed::from_bytes(&[0xbb; 32])?.unshielded_address(&network);
    let provider = MidnightProvider::new(&node);
    let wallet = WalletBuilder::new(&seed).with_network(&network).with_storage(env::var("PROBE_STORAGE")?).sync(&provider, &indexer).await?;
    let provider = provider.with_wallet(wallet);
    println!("ledger: {}", provider.ledger_version().await?);

    loop {
        let b = provider.balance().await?;
        println!("night utxos={} unregistered={} dust={} speck", b.unshielded.len(), b.dust.unregistered_night_utxos, b.dust.balance_speck);
        if b.unshielded.is_empty() { anyhow::bail!("no NIGHT on this wallet yet"); }
        if b.dust.unregistered_night_utxos == 0 { break; }
        let before = b.dust.unregistered_night_utxos;
        match provider.register_dust(None).await {
            Ok(p) => { p.wait_best().await?; println!("registered one coin"); }
            Err(e) => { println!("registration refused, waiting: {e}"); tokio::time::sleep(Duration::from_secs(20)).await; provider.resync_wallet().await?; continue; }
        }
        for _ in 0..60 {
            provider.resync_wallet().await?;
            if provider.balance().await?.dust.unregistered_night_utxos < before { break; }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
    for _ in 0..120 {
        provider.resync_wallet().await?;
        if provider.balance().await?.dust.balance_speck > 600_000_000_000_000 { break; }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    println!("dust before send: {} speck", provider.balance().await?.dust.balance_speck);
    for attempt in 1..=4 {
        match provider.transfer_unshielded(NIGHT, amount, &recipient).await {
            Ok(p) => match p.wait_finalized().await {
                Ok((inc, _)) => { println!("RESULT attempt {attempt}: {amount} STAR to {recipient} finalized: transaction {} extrinsic {:x} block {:x}", inc.transaction_hash, inc.extrinsic_hash, inc.block_hash); return Ok(()); }
                Err(e) => println!("RESULT attempt {attempt}: refused at submit: {e}"),
            },
            Err(e) => println!("RESULT attempt {attempt}: refused at build: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
        provider.resync_wallet().await?;
    }
    Ok(())
}
