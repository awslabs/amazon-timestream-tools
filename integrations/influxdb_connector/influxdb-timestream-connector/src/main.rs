#![recursion_limit = "256"]

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

use influxdb_timestream_connector::{
    lambda_handler,
    timestream_utils::{get_connection, TimestreamEnvConfig},
};
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::sync::Arc;
use tracing_subscriber::{
    filter::EnvFilter,
    fmt::{self, format::FmtSpan},
};

/// The number of threads to use to chunk Vecs in parallel
/// using rayon.
/// Lambda functions have a maximum of 1024 threads.
pub static NUM_RAYON_THREADS: usize = 32;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Configure logging
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("lambda_runtime=warn,INFO"));

    fmt::fmt()
        .with_env_filter(env_filter)
        .with_level(true)
        .with_span_events(FmtSpan::CLOSE)
        .with_target(true)
        .with_thread_names(true)
        .with_level(true)
        .compact()
        .init();

    // Set global maximum rayon threads
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_RAYON_THREADS)
        .build_global()
        .unwrap();

    // Get Timestream environment variables
    let timestream_env_config = TimestreamEnvConfig::get().await?;

    let region = timestream_env_config.region.clone();
    let timestream_client = get_connection(&region).await?;
    let timestream_client = Arc::new(timestream_client);
    run(service_fn(|event: LambdaEvent<Value>| {
        lambda_handler(&timestream_client, event)
    }))
    .await
}
