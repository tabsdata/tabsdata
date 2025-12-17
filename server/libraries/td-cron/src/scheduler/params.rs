//
// Copyright 2025 Tabs Data Inc.
//

use clap::clap_derive::{Parser, Subcommand};
use croner::Cron;
use http::Method;
use url::Url;

/// Importer binary command line parameters.
#[derive(Debug, Clone, Parser)]
pub(crate) struct Params {
    #[clap(long)]
    /// Cron mask expression.
    pub mask: Cron,

    #[clap(long)]
    /// Start of the period in which the cron mask is valid in epoch milliseconds.
    pub start: Option<i64>,

    #[clap(long)]
    /// End of the period in which the cron mask is valid in epoch milliseconds.
    pub end: Option<i64>,

    #[command(subcommand)]
    /// Task to be run in the cronjob.
    pub task: Task,
}

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum Task {
    Heartbeat {
        #[clap(long)]
        output: String,
    },

    HttpRequest {
        #[clap(long)]
        method: Method,

        #[clap(long)]
        url: Url,

        #[clap(short, long, value_parser = parse_header)]
        headers: Vec<(String, String)>,

        #[clap(long)]
        body: Option<String>,
    },
}

/// Parse a single header argument `key=value` into a tuple
fn parse_header(s: &str) -> Result<(String, String), String> {
    let parts = s.splitn(2, '=').collect::<Vec<_>>();
    if parts.len() == 2 {
        Ok((parts[0].to_string(), parts[1].to_string()))
    } else {
        Err(format!("Invalid header format: '{}'. Use key=value", s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_header() {
        assert_eq!(
            parse_header("key=value"),
            Ok(("key".to_string(), "value".to_string()))
        );
        assert_eq!(
            parse_header("key=value=another"),
            Ok(("key".to_string(), "value=another".to_string()))
        );
        assert!(parse_header("invalid").is_err());
    }
}
