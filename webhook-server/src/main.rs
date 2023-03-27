use clap::Parser;

extern crate webhook_server_lib;
use webhook_server_lib::config::Args;

fn set_up_logging(args: &Args) {
    log4rs::init_file(&args.log_file, Default::default()).unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    println!("config: {:?}", args);
    set_up_logging(&args);
    webhook_server_lib::server_main(args).await
}