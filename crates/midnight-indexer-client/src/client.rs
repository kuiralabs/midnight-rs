use std::time::Duration;

use serde::{Serialize, de::DeserializeOwned};
use tracing::{debug, warn};

use crate::endpoint::IndexerEndpoint;
use crate::error::IndexerError;
use crate::queries;
use crate::types::*;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// The self-hosted indexer's GraphQL path under a base URL. A caller that
/// names `/api/v3/graphql` itself still gets it: that path is an alias of v4,
/// and rewriting a URL a caller chose is worse than serving it.
const GRAPHQL_PATH: &str = "/api/v4/graphql";

/// The most bytes of an undecodable response body that the warning logs.
const LOGGED_BODY_BYTES: usize = 512;

/// Returns the GraphQL endpoint for an indexer URL.
///
/// Keeps a URL whose path ends in `/graphql`, and drops a `/ws` that follows
/// `/graphql`. Appends [`GRAPHQL_PATH`] to any other URL.
pub(crate) fn graphql_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    let base = match base.strip_suffix("/ws") {
        Some(graphql) if graphql.ends_with("/graphql") => graphql,
        _ => base,
    };
    if base.ends_with("/graphql") {
        base.to_owned()
    } else {
        format!("{base}{GRAPHQL_PATH}")
    }
}

pub struct IndexerClient {
    http: reqwest::Client,
    endpoint: IndexerEndpoint,
}

impl IndexerClient {
    /// Create a new client.
    ///
    /// A bare URL takes the self-hosted indexer's layout (see
    /// [`IndexerEndpoint::from_base`]): the base indexer URL (e.g.
    /// `http://127.0.0.1:8088`), or a URL whose path ends in `/graphql` or
    /// `/graphql/ws`. The client sends queries to that `/graphql` path, or to
    /// `/api/v4/graphql` under a base URL. An [`IndexerEndpoint`] is used as
    /// given, and its credential is sent as a header on every query. Returns
    /// an error if the HTTP client cannot be built.
    pub fn new(endpoint: impl Into<IndexerEndpoint>) -> Result<Self, IndexerError> {
        let endpoint = endpoint.into();

        let http = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(|e| IndexerError::Config(e.to_string()))?;

        Ok(Self { http, endpoint })
    }

    /// URL the client posts GraphQL to.
    pub fn url(&self) -> &str {
        self.endpoint.graphql_url()
    }

    /// The endpoint this client was built for.
    pub fn endpoint(&self) -> &IndexerEndpoint {
        &self.endpoint
    }

    async fn execute<V: Serialize, R: DeserializeOwned>(
        &self,
        query: &str,
        variables: V,
    ) -> Result<R, IndexerError> {
        let body = serde_json::json!({
            "query": query,
            "variables": variables,
        });

        debug!(url = %self.url(), "executing indexer GraphQL query");

        let mut request = self.http.post(self.url()).json(&body);
        if let Some(credential) = self.endpoint.credential() {
            request = request.header(credential.header(), credential.value());
        }
        let resp = request.send().await?.error_for_status()?;

        let text = resp.text().await?;

        let gql_resp: GraphQLResponse<R> = serde_json::from_str(&text).map_err(|e| {
            let logged = &text[..text.floor_char_boundary(LOGGED_BODY_BYTES)];
            warn!(error = %e, body = logged, bytes = text.len(), "failed to deserialize indexer response");
            IndexerError::Deserialization(e.to_string())
        })?;

        gql_resp.into_result()?.ok_or(IndexerError::MissingData)
    }

    // -- Blocks --

    /// Fetch a block by optional offset. Returns the latest block when
    /// `offset` is `None`.
    pub async fn get_block(
        &self,
        offset: Option<BlockOffset>,
    ) -> Result<Option<Block>, IndexerError> {
        let vars = BlockQueryVars { offset };
        let data: BlockQueryData = self.execute(queries::BLOCK_QUERY, vars).await?;
        Ok(data.block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_construction_bare_host() {
        let client = IndexerClient::new("http://localhost:8088").unwrap();
        assert_eq!(client.url(), "http://localhost:8088/api/v4/graphql");
    }

    #[test]
    fn url_construction_with_trailing_slash() {
        let client = IndexerClient::new("http://localhost:8088/").unwrap();
        assert_eq!(client.url(), "http://localhost:8088/api/v4/graphql");
    }

    #[test]
    fn url_construction_full_path() {
        let client = IndexerClient::new("http://localhost:8088/api/v3/graphql").unwrap();
        assert_eq!(client.url(), "http://localhost:8088/api/v3/graphql");
    }

    #[test]
    fn url_construction_full_v4_path() {
        let client = IndexerClient::new("http://localhost:8088/api/v4/graphql").unwrap();
        assert_eq!(client.url(), "http://localhost:8088/api/v4/graphql");
    }

    #[test]
    fn graphql_url_drops_ws_only_after_graphql() {
        assert_eq!(
            graphql_url("http://localhost:8088/graphql/ws"),
            "http://localhost:8088/graphql"
        );
        assert_eq!(
            graphql_url("http://localhost:8088/foo/ws"),
            "http://localhost:8088/foo/ws/api/v4/graphql"
        );
    }
}
