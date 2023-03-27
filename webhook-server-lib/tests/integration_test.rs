#![feature(result_flattening)]

use std::path::{PathBuf, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use std::net::TcpListener;

use tokio::time::{sleep, Duration};

extern crate webhook_server_lib;
use webhook_server_lib::config::Args;
use webhook_server_lib::templates::Templates;

pub struct Server {
    started: AtomicBool,
    args: Args,
    templates: Templates,
}

impl Server {
    pub fn new() -> Server {
        let args = Self::args();
        let templates = Self::templates(&args);
        Server {
            started: AtomicBool::new(false),
            args: args,
            templates: templates,
        }
    }

    fn args() -> Args {
        let bound = TcpListener::bind(("0.0.0.0", 0)).unwrap();
        let port = bound.local_addr().unwrap().port();
        drop(bound);

        let (templates_file, log_config) = Self::find_configs();
        println!("Found templates file: {}", templates_file);
        println!("Found log config: {}", log_config);

        Args {
            log_file: log_config,
            address: String::from("0.0.0.0"),
            port: port,
            templates_file: templates_file,
        }
    }

    ///Returns the templates file and log configuration file
    fn find_configs() -> (String, String) {
        let path = std::env::current_dir().expect("failed getting current directory");
        println!("Current dir: {}", path.display());

        let test_templates = Path::new("test-templates.yaml");

        let top_level = path.join(test_templates);
        println!("Looking in: {}", top_level.display());
        if top_level.exists() {
            return Self::files(path);
        }

        let tests_dir = path.join(Path::new("tests"));
        let tests_level = tests_dir.join(test_templates);
        println!("Looking in: {}", tests_level.display());
        if tests_level.exists() {
            return Self::files(tests_dir);
        }

        let lib_tests_dir = path.join(Path::new("webhook-server-lib/tests"));
        let lib_tests_level = lib_tests_dir.join(test_templates);
        println!("Looking in: {}", lib_tests_level.display());
        if lib_tests_level.exists() {
            return Self::files(lib_tests_dir);
        }
        panic!("Failed to find the configuration");
    }

    fn files(mut location: PathBuf) -> (String, String) {
        location.push("test-templates.yaml");
        let templates_location = String::from(location.to_str().expect("template file not convertable"));

        location.pop();
        location.push("test-log4rs.yml");

        let log_config = String::from(location.to_str().expect("log config not convertable"));

        (templates_location, log_config)
    }

    fn templates(args: &Args) -> Templates {
        Templates::from_file(&args.templates_file).unwrap()
    }

    pub async fn init_server(&self) {
        if !self.started.load(Ordering::Relaxed) {
            self.started.store(true, Ordering::Relaxed);

            let args = self.args.clone();
            let templates = self.templates.clone();

            tokio::spawn(async move {
                let rt = tokio::runtime::Runtime::new().expect("failed starting runtime");
                rt.spawn(webhook_server_lib::run_server(args, templates));
                loop {
                    sleep(Duration::from_millis(100_000)).await;
                }
            });
            sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test]
async fn test_try_posting_returned_from_root() {
    //Starting server
    let server = Server::new();
    server.init_server().await;
    println!("Server is listening on port: {}", server.args.port);

    let url = format!("http://localhost:{}", server.args.port);
    let resp = match reqwest::get(url).await {
        Ok(r) => r.text().await,
        Err(e) => Err(e),
    };
    match resp {
        Ok(r) => assert_eq!(true, r.starts_with("Try POST")),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}