use jj_cli::cli_util::CliRunner;

fn main() -> std::process::ExitCode {
    CliRunner::init()
        .version(env!("CARGO_PKG_VERSION"))
        .add_store_factories(cumulus_backend::store_factories())
        .run()
        .into()
}
