use std::net::{SocketAddr, IpAddr};

use clap::Parser;
use http_body_util::{Full, combinators::BoxBody, Empty, BodyExt};
use hyper::{Request, Response, body::{Bytes, Incoming, Frame}, server::conn::http1, service::service_fn, Method, StatusCode};
use tokio::net::TcpListener;

mod resource;
mod templates;

#[derive(Parser,Debug)]
struct Args {
    #[arg(short, long, default_value_t = String::from("log4rs.yml"))]
    log_file: String,
    #[arg(short, long, default_value_t = String::from("127.0.0.1"))]
    address: String,
    #[arg(short, long, default_value_t = 3000)]
    port: u16,
}

fn set_up_logging(args: &Args) {
    log4rs::init_file(&args.log_file, Default::default()).unwrap();
}

async fn echo(
    req: Request<Incoming>
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(full(
            "Try POSTing data to /echo"
        ))),
        (&Method::POST, "/echo") => {
            Ok(Response::new(req.into_body().boxed()))
        },
        (&Method::POST, "/echo/uppercase") => {
            let frame_stream = req.into_body().map_frame(|frame| {
                let frame = if let Ok(data) = frame.into_data() {
                    data.iter()
                        .map(|byte| byte.to_ascii_uppercase())
                        .collect::<Bytes>()
                } else {
                    Bytes::new()
                };
                Frame::data(frame)
            });
            Ok(Response::new(frame_stream.boxed()))
        },

        _ => {
            let mut not_found = Response::new(empty());
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

async fn run_server(args: &Args) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log::info!("Setting up server on {}:{}", args.address, args.port);
    let ip_addr: IpAddr = args.address.parse().unwrap();
    let addr = SocketAddr::new(ip_addr, args.port);

    let listener = TcpListener::bind(addr).await?;

    loop {
        let (stream, _) = listener.accept().await?;

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(stream, service_fn(echo))
                .await
            {
                log::error!("Error serving connection: {:?}", err);
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    println!("config: {:?}", args);
    set_up_logging(&args);
    run_server(&args).await
}
