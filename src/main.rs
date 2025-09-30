use anyhow::Result;
use clap::Parser;
use tracing::{info, Level};
use tracing_subscriber::{fmt, EnvFilter};

mod cli;
mod discover;
mod error;
mod schema;
mod csv_in;
mod parquet_in;
mod writer_csv;
mod writer_parquet;
mod coercion;
mod pipeline;
mod state;
mod progress;

use cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let filter = if cli.verbose > 0 {
        let level = match cli.verbose {
            1 => Level::DEBUG,
            2 => Level::TRACE,
            _ => Level::TRACE,
        };
        EnvFilter::new(format!("maw={}", level))
    } else if cli.quiet {
        EnvFilter::new("warn")
    } else {
        EnvFilter::from_default_env()
    };

    if cli.json_logs {
        let subscriber = fmt().json().with_env_filter(filter).finish();
        tracing::subscriber::set_global_default(subscriber)?;
    } else {
        let subscriber = fmt().with_env_filter(filter).finish();
        tracing::subscriber::set_global_default(subscriber)?;
    }

    info!("Starting maw v{}", env!("CARGO_PKG_VERSION"));

    // Execute the main logic
    match execute(cli).await {
        Ok(()) => {
            info!("Operation completed successfully");
            Ok(())
        }
        Err(e) => {
            tracing::error!("Operation failed: {}", e);
            std::process::exit(1);
        }
    }
}

async fn execute(cli: Cli) -> Result<()> {
    use crate::pipeline::Pipeline;
    
    if cli.plan {
        info!("Plan mode: would process {} inputs", cli.inputs.len());
        for input in &cli.inputs {
            info!("  - {}", input);
        }
        return Ok(());
    }

    if cli.dry_run {
        info!("Dry run mode: would process inputs without writing output");
        return Ok(());
    }

    // Create and execute the pipeline
    let pipeline = Pipeline::new(cli);
    pipeline.execute().await?;
    
    Ok(())
}
