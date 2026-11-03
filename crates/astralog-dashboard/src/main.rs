#![forbid(unsafe_code)]

use astralog_dashboard::{CliAction, DashboardConfig, help_text};

#[tokio::main]
async fn main() {
    let args = std::env::args().skip(1);
    let config = match DashboardConfig::parse_from(args) {
        Ok(CliAction::Run(config)) => config,
        Ok(CliAction::Help) => {
            print!("{}", help_text());
            return;
        }
        Err(error) => {
            eprintln!("error: {error}\n\n{}", help_text());
            std::process::exit(2);
        }
    };

    if let Err(error) = astralog_dashboard::run(config).await {
        eprintln!("dashboard adapter failed: {error}");
        std::process::exit(1);
    }
}
