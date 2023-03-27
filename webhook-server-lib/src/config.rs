use clap::Parser;

#[derive(Parser,Debug,Clone)]
pub struct Args {
    #[arg(short, long, default_value_t = String::from("log4rs.yml"))]
    pub log_file: String,
    #[arg(short, long, default_value_t = String::from("127.0.0.1"))]
    pub address: String,
    #[arg(short, long, default_value_t = 3000)]
    pub port: u16,
    #[arg(short, long, default_value_t = String::from("templates.yaml"))]
    pub templates_file: String,
}
