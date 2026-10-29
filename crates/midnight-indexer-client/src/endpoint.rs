//! Where an indexer serves GraphQL, where it serves subscriptions, and what it
//! needs to let a client in.
//!
//! The self-hosted Midnight indexer serves both under one versioned base, so a
//! base URL is enough to find them: `{base}/api/v4/graphql` and
//! `{base}/api/v4/graphql/ws`. Other operators lay the same API out
//! differently. Blockfrost serves GraphQL at its API root and subscriptions at
//! `/ws`, and authenticates every request. Appending the self-hosted paths to
//! such a host yields a 404 that reads like an unrelated fault, so an endpoint
//! built with [`IndexerEndpoint::new`] is used exactly as given.
//!
//! A credential travels as a request header, never in the URL. URLs end up in
//! logs and error messages, and a URL that carries no secret cannot leak one.

use std::fmt;

/// A header an indexer requires on every request, such as Blockfrost's
/// `project_id`.
///
/// `Debug` and `Display` print the header name and never the value.
#[derive(Clone, PartialEq, Eq)]
pub struct IndexerCredential {
    header: String,
    value: String,
}

impl IndexerCredential {
    pub fn new(header: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            header: header.into(),
            value: value.into(),
        }
    }

    pub fn header(&self) -> &str {
        &self.header
    }

    /// The secret itself. Only the request builders read it.
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Debug for IndexerCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexerCredential({}=…)", self.header)
    }
}

impl fmt::Display for IndexerCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}=…", self.header)
    }
}

/// The two URLs of one indexer and the credential it requires, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexerEndpoint {
    graphql_url: String,
    ws_url: String,
    credential: Option<IndexerCredential>,
}

impl IndexerEndpoint {
    /// An endpoint used exactly as given: nothing is appended to either URL.
    ///
    /// `ws_url` may use an `http(s)` scheme; it is converted to `ws(s)`.
    pub fn new(
        graphql_url: impl Into<String>,
        ws_url: impl Into<String>,
        credential: Option<IndexerCredential>,
    ) -> Self {
        Self {
            graphql_url: graphql_url.into().trim_end_matches('/').to_string(),
            ws_url: to_ws_scheme(ws_url.into().trim_end_matches('/')),
            credential,
        }
    }

    /// The self-hosted Midnight indexer's layout, from a base URL or from
    /// either of its full endpoints.
    ///
    /// A base gains [`crate::GRAPHQL_PATH`] and [`crate::GRAPHQL_WS_PATH`]. A
    /// URL already ending in `/graphql` or `/graphql/ws` is honoured as the
    /// caller wrote it, including the `/api/v3` alias.
    pub fn from_base(url: &str) -> Self {
        let base = url.trim_end_matches('/');
        let (graphql_url, ws_url) = if let Some(graphql) = base.strip_suffix("/ws") {
            if graphql.ends_with("/graphql") {
                (to_http_scheme(graphql), base.to_string())
            } else {
                (
                    format!("{base}{}", crate::GRAPHQL_PATH),
                    format!("{base}{}", crate::GRAPHQL_WS_PATH),
                )
            }
        } else if base.ends_with("/graphql") {
            (base.to_string(), format!("{base}/ws"))
        } else {
            (
                format!("{base}{}", crate::GRAPHQL_PATH),
                format!("{base}{}", crate::GRAPHQL_WS_PATH),
            )
        };
        Self {
            graphql_url,
            ws_url: to_ws_scheme(&ws_url),
            credential: None,
        }
    }

    pub fn graphql_url(&self) -> &str {
        &self.graphql_url
    }

    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }

    pub fn credential(&self) -> Option<&IndexerCredential> {
        self.credential.as_ref()
    }
}

impl From<&str> for IndexerEndpoint {
    fn from(url: &str) -> Self {
        Self::from_base(url)
    }
}

impl From<String> for IndexerEndpoint {
    fn from(url: String) -> Self {
        Self::from_base(&url)
    }
}

impl From<&String> for IndexerEndpoint {
    fn from(url: &String) -> Self {
        Self::from_base(url)
    }
}

impl From<&IndexerEndpoint> for IndexerEndpoint {
    fn from(endpoint: &IndexerEndpoint) -> Self {
        endpoint.clone()
    }
}

fn to_ws_scheme(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        url.to_string()
    }
}

fn to_http_scheme(url: &str) -> String {
    if let Some(rest) = url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else if let Some(rest) = url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else {
        url.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_base_gains_the_self_hosted_paths() {
        let e = IndexerEndpoint::from_base("http://localhost:8088/");
        assert_eq!(e.graphql_url(), "http://localhost:8088/api/v4/graphql");
        assert_eq!(e.ws_url(), "ws://localhost:8088/api/v4/graphql/ws");
        assert!(e.credential().is_none());
    }

    #[test]
    fn a_full_self_hosted_endpoint_is_honoured_from_either_end() {
        for url in [
            "https://indexer.preview.midnight.network/api/v4/graphql",
            "wss://indexer.preview.midnight.network/api/v4/graphql/ws",
        ] {
            let e = IndexerEndpoint::from_base(url);
            assert_eq!(
                e.graphql_url(),
                "https://indexer.preview.midnight.network/api/v4/graphql",
                "{url}"
            );
            assert_eq!(
                e.ws_url(),
                "wss://indexer.preview.midnight.network/api/v4/graphql/ws",
                "{url}"
            );
        }
        let v3 = IndexerEndpoint::from_base("http://localhost:8088/api/v3/graphql");
        assert_eq!(v3.graphql_url(), "http://localhost:8088/api/v3/graphql");
        assert_eq!(v3.ws_url(), "ws://localhost:8088/api/v3/graphql/ws");
    }

    /// Blockfrost's layout. HTTP and WS are asserted as a pair: getting one of
    /// them wrong is the failure that hides, because the other keeps working.
    #[test]
    fn an_explicit_endpoint_is_used_exactly_as_given() {
        let e = IndexerEndpoint::new(
            "https://midnight-preprod.blockfrost.io/api/v0",
            "https://midnight-preprod.blockfrost.io/api/v0/ws",
            Some(IndexerCredential::new("project_id", "nightpreprodSECRET")),
        );
        assert_eq!(
            e.graphql_url(),
            "https://midnight-preprod.blockfrost.io/api/v0"
        );
        assert_eq!(e.ws_url(), "wss://midnight-preprod.blockfrost.io/api/v0/ws");
        assert_eq!(e.credential().unwrap().value(), "nightpreprodSECRET");
    }

    #[test]
    fn no_rendering_of_an_endpoint_carries_the_secret() {
        let e = IndexerEndpoint::new(
            "https://h/api/v0",
            "https://h/api/v0/ws",
            Some(IndexerCredential::new("project_id", "nightpreprodSECRET")),
        );
        for rendered in [
            format!("{e:?}"),
            format!("{:?}", e.credential().unwrap()),
            format!("{}", e.credential().unwrap()),
            e.graphql_url().to_string(),
            e.ws_url().to_string(),
        ] {
            assert!(!rendered.contains("SECRET"), "{rendered}");
        }
        assert!(format!("{e:?}").contains("project_id"));
    }
}
