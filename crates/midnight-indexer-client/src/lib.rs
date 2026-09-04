/// Path component of every indexer GraphQL endpoint, including the API version segment.
///
/// Single source of truth: when the indexer cuts a new major, this constant is the only line that
/// changes. It was previously spelled out in five places across `client.rs` and `subscription.rs`,
/// which is how the client sat on `v3` long after the Android SDK moved to `v4`.
///
/// `/api/v3` is currently an alias for `/api/v4` in the indexer, so the two behave identically
/// today — but an alias is a deprecation shim, and the version the client actually asks for should
/// be the one it means.
pub const API_PATH: &str = "/api/v4";

/// The GraphQL endpoint under [`API_PATH`].
pub const GRAPHQL_PATH: &str = "/api/v4/graphql";

/// The GraphQL subscription endpoint under [`API_PATH`].
pub const GRAPHQL_WS_PATH: &str = "/api/v4/graphql/ws";

mod client;
mod error;
pub mod queries;
pub mod subscription;
#[cfg(feature = "test-util")]
pub mod testutil;
pub mod types;

pub use client::IndexerClient;
pub use error::IndexerError;
pub use subscription::{Subscription, SubscriptionClient};
pub use types::*;
