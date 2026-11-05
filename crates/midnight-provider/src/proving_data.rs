//! The files local proving reads that no contract carries: the built-in Zswap
//! and Dust keys of both linked ledger generations, and the universal
//! parameters (`bls_midnight_2p<k>`) for circuits of `2^k` rows.
//!
//! The prover fetches each of these the first time it needs it. Provisioning
//! fetches them all ahead of that, so a device can prove with no network, and
//! prefers a copy an app bundled over a download. Every file is checked against
//! the SHA-256 the ledger expects before it counts as present; a cached or
//! bundled copy that fails the check is replaced.

use std::{
    io,
    path::{Path, PathBuf},
};

use midnight_helpers::{ledger_8, ledger_9};

/// The smallest universal parameters provisioned. A contract circuit proves
/// with the parameters sized to it exactly, and a trivial one needs `2^5` rows.
pub const MIN_CIRCUIT_K: u8 = 5;

/// The largest parameters provisioned unless more are asked for. Covers the
/// Zswap and Dust circuits (`k` 13 to 15) and every contract circuit under
/// `2^15` rows.
pub const BASELINE_MAX_CIRCUIT_K: u8 = 15;

/// The largest parameters that may be asked for: the biggest circuit measured
/// to prove on a phone. Each step doubles the file (12 MB at 16, 48 MB at 18).
pub const ABSOLUTE_MAX_CIRCUIT_K: u8 = 18;

/// Where a provisioned file came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Provisioned {
    /// The cache already held a copy with the expected hash.
    Present,
    /// Copied from the bundle.
    Installed,
    /// Downloaded.
    Downloaded,
}

/// How many files each source supplied.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProvisionReport {
    pub present: usize,
    pub installed: usize,
    pub downloaded: usize,
}

impl ProvisionReport {
    fn record(&mut self, outcome: Provisioned) {
        match outcome {
            Provisioned::Present => self.present += 1,
            Provisioned::Installed => self.installed += 1,
            Provisioned::Downloaded => self.downloaded += 1,
        }
    }
}

/// A file local proving reads, and the generation whose data provider knows
/// its hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProvingFile {
    /// The path under the cache, which is also its path under the source URL.
    pub name: String,
    generation: Generation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Generation {
    Ledger8,
    Ledger9,
}

/// Every file local proving reads that no contract carries, with universal
/// parameters up to `2^max_k` rows.
pub fn wallet_proving_files(max_k: u8) -> io::Result<Vec<ProvingFile>> {
    if !(BASELINE_MAX_CIRCUIT_K..=ABSOLUTE_MAX_CIRCUIT_K).contains(&max_k) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "the largest circuit size must be between {BASELINE_MAX_CIRCUIT_K} and \
                 {ABSOLUTE_MAX_CIRCUIT_K}, got {max_k}: below {BASELINE_MAX_CIRCUIT_K} drops \
                 parameters the wallet's own circuits need, and above \
                 {ABSOLUTE_MAX_CIRCUIT_K} is a size no phone has been measured to prove"
            ),
        ));
    }
    // Generations may share a key: a release candidate can keep the previous
    // generation's Zswap and Dust keys under the same names. One cache holds one
    // copy of a name, so a shared name is listed once, and two generations that
    // expect different bytes under it cannot share a cache at all.
    let mut files: Vec<ProvingFile> = Vec::new();
    let mut hashes: Vec<[u8; 32]> = Vec::new();
    let generations = [
        (ledger_8::ZSWAP_EXPECTED_FILES, Generation::Ledger8),
        (ledger_8::DUST_EXPECTED_FILES, Generation::Ledger8),
        (ledger_9::ZSWAP_EXPECTED_FILES, Generation::Ledger9),
        (ledger_9::DUST_EXPECTED_FILES, Generation::Ledger9),
    ];
    for (expected, generation) in generations {
        for (name, hash, _) in expected {
            match files.iter().position(|f| f.name == *name) {
                Some(i) if hashes[i] == *hash => {}
                Some(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "the linked ledger generations expect different bytes under {name}"
                        ),
                    ));
                }
                None => {
                    files.push(ProvingFile {
                        name: (*name).to_owned(),
                        generation,
                    });
                    hashes.push(*hash);
                }
            }
        }
    }
    files.extend((MIN_CIRCUIT_K..=max_k).map(|k| ProvingFile {
        name: ledger_9::MidnightDataProvider::name_k(k),
        generation: Generation::Ledger9,
    }));
    Ok(files)
}

/// Makes sure `cache` holds every file of [`wallet_proving_files`] with its
/// expected hash, taking each from the cache, then `bundle`, then the network
/// (`MIDNIGHT_PARAM_SOURCE`, default `https://srs.midnight.network/`).
///
/// `cache` must be the directory the prover reads, `MIDNIGHT_PP`. `bundle` is
/// laid out as the cache is. `progress` is told the files done and the total
/// after each file.
pub async fn provision_wallet_proving_files(
    cache: &Path,
    bundle: Option<&Path>,
    max_k: u8,
    progress: impl FnMut(usize, usize),
) -> io::Result<ProvisionReport> {
    let files = wallet_proving_files(max_k)?;
    let sources = Sources::new(cache.to_path_buf(), None);
    provision(&sources, &files, bundle, progress).await
}

async fn provision(
    sources: &Sources,
    files: &[ProvingFile],
    bundle: Option<&Path>,
    mut progress: impl FnMut(usize, usize),
) -> io::Result<ProvisionReport> {
    let mut report = ProvisionReport::default();
    for (done, file) in files.iter().enumerate() {
        report.record(provision_one(sources, file, bundle).await?);
        progress(done + 1, files.len());
    }
    Ok(report)
}

async fn provision_one(
    sources: &Sources,
    file: &ProvingFile,
    bundle: Option<&Path>,
) -> io::Result<Provisioned> {
    let path = sources.cache.join(&file.name);
    // With the file in the cache, the data provider reads and hashes it and
    // never touches the network.
    if path.is_file() && sources.verified(file, &path).await? {
        return Ok(Provisioned::Present);
    }
    if let Some(source) = bundle.map(|b| b.join(&file.name)).filter(|s| s.is_file()) {
        copy_atomically(&source, &path)?;
        if sources.verified(file, &path).await? {
            return Ok(Provisioned::Installed);
        }
        tracing::warn!(file = %file.name, "the bundled copy does not have the expected hash; downloading it");
    }
    sources.fetch(file).await.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "{} is not in the cache or the bundle, and downloading it failed: {e}",
                file.name
            ),
        )
    })?;
    Ok(Provisioned::Downloaded)
}

/// The data providers of both generations, over one cache.
struct Sources {
    cache: PathBuf,
    ledger_8: ledger_8::MidnightDataProvider,
    ledger_9: ledger_9::MidnightDataProvider,
}

impl Sources {
    fn new(cache: PathBuf, base_url: Option<&str>) -> Self {
        let ledger_8 = ledger_8::MidnightDataProvider {
            fetch_mode: ledger_8::FetchMode::OnDemand,
            base_url: base_url
                .map(|u| u.parse().expect("a valid URL"))
                .unwrap_or_else(|| ledger_8::base_crypto::data_provider::BASE_URL.clone()),
            output_mode: ledger_8::OutputMode::Log,
            expected_data: ledger_8::ZSWAP_EXPECTED_FILES
                .iter()
                .chain(ledger_8::DUST_EXPECTED_FILES)
                .copied()
                .collect(),
            dir: cache.clone(),
        };
        let ledger_9 = ledger_9::MidnightDataProvider {
            fetch_mode: ledger_9::FetchMode::OnDemand,
            base_url: base_url
                .map(|u| u.parse().expect("a valid URL"))
                .unwrap_or_else(|| ledger_9::base_crypto::data_provider::BASE_URL.clone()),
            output_mode: ledger_9::OutputMode::Log,
            expected_data: ledger_9::ZSWAP_EXPECTED_FILES
                .iter()
                .chain(ledger_9::DUST_EXPECTED_FILES)
                .copied()
                .collect(),
            dir: cache.clone(),
        };
        Self {
            cache,
            ledger_8,
            ledger_9,
        }
    }

    async fn fetch(&self, file: &ProvingFile) -> io::Result<()> {
        match file.generation {
            Generation::Ledger8 => self.ledger_8.fetch(&file.name).await,
            Generation::Ledger9 => self.ledger_9.fetch(&file.name).await,
        }
    }

    /// Whether the copy at `path` has the expected hash. A copy that does not
    /// is removed, so the next source can replace it.
    async fn verified(&self, file: &ProvingFile, path: &Path) -> io::Result<bool> {
        match self.fetch(file).await {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                std::fs::remove_file(path)?;
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }
}

/// Copies through a temporary file in the same directory, so the cache never
/// holds a partial copy.
fn copy_atomically(source: &Path, destination: &Path) -> io::Result<()> {
    let parent = destination.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no parent", destination.display()),
        )
    })?;
    std::fs::create_dir_all(parent)?;
    let temporary = parent.join(format!(
        ".{}.partial",
        destination
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default()
    ));
    std::fs::copy(source, &temporary)?;
    std::fs::rename(&temporary, destination)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A host nothing listens on: any fetch from it fails, so a test that
    /// passes with it never touched the network.
    const NO_NETWORK: &str = "http://127.0.0.1:9/";

    fn small_files() -> Vec<ProvingFile> {
        let all = wallet_proving_files(BASELINE_MAX_CIRCUIT_K).unwrap();
        ["bls_midnight_2p5", "dust/", "zswap/"]
            .iter()
            .map(|prefix| {
                all.iter()
                    .filter(|f| {
                        f.name.starts_with(prefix)
                            && f.name.ends_with(if prefix.ends_with('/') {
                                ".verifier"
                            } else {
                                ""
                            })
                    })
                    .min_by_key(|f| f.name.clone())
                    .cloned()
                    .unwrap()
            })
            .collect()
    }

    fn temp(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("proving-data-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Fills a bundle from the network once, for the tests that must not use it.
    async fn downloaded_bundle(label: &str, files: &[ProvingFile]) -> PathBuf {
        let bundle = temp(label);
        let report = provision(&Sources::new(bundle.clone(), None), files, None, |_, _| {})
            .await
            .unwrap();
        assert_eq!(report.downloaded, files.len());
        bundle
    }

    #[test]
    fn every_linked_key_and_the_parameter_ladder_are_listed_once() {
        let names: Vec<_> = wallet_proving_files(16)
            .unwrap()
            .into_iter()
            .map(|f| f.name)
            .collect();
        for expected in ["bls_midnight_2p5", "bls_midnight_2p16"] {
            assert!(
                names.contains(&expected.to_owned()),
                "{expected} missing from {names:?}"
            );
        }
        assert!(!names.contains(&"bls_midnight_2p4".to_owned()));
        assert!(!names.contains(&"bls_midnight_2p17".to_owned()));
        let linked = ledger_8::ZSWAP_EXPECTED_FILES
            .iter()
            .chain(ledger_8::DUST_EXPECTED_FILES)
            .chain(ledger_9::ZSWAP_EXPECTED_FILES)
            .chain(ledger_9::DUST_EXPECTED_FILES);
        for (name, ..) in linked {
            assert!(
                names.contains(&(*name).to_owned()),
                "{name} missing from {names:?}"
            );
        }
        let unique: std::collections::BTreeSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len(), "a name listed twice: {names:?}");
    }

    #[test]
    fn a_size_outside_the_measured_range_is_refused() {
        for k in [BASELINE_MAX_CIRCUIT_K - 1, ABSOLUTE_MAX_CIRCUIT_K + 1] {
            let error = wallet_proving_files(k).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
        }
    }

    #[tokio::test]
    async fn a_bundle_provisions_an_empty_cache_with_no_network() {
        let files = small_files();
        let bundle = downloaded_bundle("bundle-a", &files).await;
        let cache = temp("cache-a");

        let report = provision(
            &Sources::new(cache.clone(), Some(NO_NETWORK)),
            &files,
            Some(&bundle),
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(
            report,
            ProvisionReport {
                present: 0,
                installed: files.len(),
                downloaded: 0
            }
        );

        let again = provision(
            &Sources::new(cache.clone(), Some(NO_NETWORK)),
            &files,
            None,
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(
            again,
            ProvisionReport {
                present: files.len(),
                installed: 0,
                downloaded: 0
            }
        );
        for file in &files {
            assert_eq!(
                std::fs::read(cache.join(&file.name)).unwrap(),
                std::fs::read(bundle.join(&file.name)).unwrap()
            );
        }
    }

    /// A cached copy with the wrong bytes would fail every proof that reads it;
    /// provisioning replaces it rather than counting it present.
    #[tokio::test]
    async fn a_corrupt_cached_copy_is_replaced_from_the_bundle() {
        let files = small_files();
        let bundle = downloaded_bundle("bundle-b", &files).await;
        let cache = temp("cache-b");
        let victim = cache.join(&files[0].name);
        std::fs::create_dir_all(victim.parent().unwrap()).unwrap();
        std::fs::write(&victim, b"not the parameters").unwrap();

        let report = provision(
            &Sources::new(cache.clone(), Some(NO_NETWORK)),
            &files,
            Some(&bundle),
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(report.installed, files.len());
        assert_eq!(
            std::fs::read(&victim).unwrap(),
            std::fs::read(bundle.join(&files[0].name)).unwrap()
        );
    }

    #[tokio::test]
    async fn a_corrupt_bundled_copy_is_downloaded_instead() {
        let files = small_files();
        let bundle = downloaded_bundle("bundle-c", &files).await;
        std::fs::write(bundle.join(&files[1].name), b"tampered").unwrap();
        let cache = temp("cache-c");

        let report = provision(
            &Sources::new(cache.clone(), None),
            &files,
            Some(&bundle),
            |_, _| {},
        )
        .await
        .unwrap();
        assert_eq!(
            report,
            ProvisionReport {
                present: 0,
                installed: files.len() - 1,
                downloaded: 1
            }
        );
        assert_ne!(
            std::fs::read(cache.join(&files[1].name)).unwrap(),
            b"tampered"
        );
    }

    #[tokio::test]
    async fn with_no_bundle_and_no_network_provisioning_fails() {
        let files = small_files();
        let cache = temp("cache-d");
        let error = provision(
            &Sources::new(cache.clone(), Some(NO_NETWORK)),
            &files,
            None,
            |_, _| {},
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().starts_with(&format!(
                "{} is not in the cache or the bundle",
                files[0].name
            )),
            "{error}"
        );
        assert!(
            !cache.join(&files[0].name).exists(),
            "a failed download must leave nothing behind"
        );
    }

    #[tokio::test]
    async fn progress_counts_every_file() {
        let files = small_files();
        let bundle = downloaded_bundle("bundle-e", &files).await;
        let mut seen = vec![];
        provision(
            &Sources::new(temp("cache-e"), Some(NO_NETWORK)),
            &files,
            Some(&bundle),
            |done, total| seen.push((done, total)),
        )
        .await
        .unwrap();
        assert_eq!(
            seen,
            (1..=files.len())
                .map(|d| (d, files.len()))
                .collect::<Vec<_>>()
        );
    }
}
