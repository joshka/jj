//! cumulusd: the Cumulus object/op server for Jujutsu (`cumulus/docs/SPEC.md` §6).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use cumulus_server::build_router;
use cumulus_server::config::Config;

/// A single static binary serving multiple Cumulus repos over gRPC.
#[derive(Debug, Parser)]
#[command(name = "cumulusd", version)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = %err, "cumulusd failed");
            eprintln!("cumulusd: {err}");
            let mut source = err.source();
            while let Some(err) = source {
                eprintln!("  caused by: {err}");
                source = err.source();
            }
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = Config::load(&args.config)?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let router = build_router(&config)?;
        tracing::info!(
            listen_addr = %config.listen_addr,
            data_dir = %config.data_dir.display(),
            auth = !config.auth.tokens.is_empty(),
            tls = config.tls.is_some(),
            "cumulusd listening"
        );
        router.serve(config.listen_addr).await?;
        Ok(())
    })
}
