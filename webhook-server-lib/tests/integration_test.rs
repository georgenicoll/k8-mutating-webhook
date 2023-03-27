mod test_server;
use test_server::TestServer;

#[tokio::test]
async fn test_try_posting_returned_from_root() {
    //Starting server
    let server = TestServer::new();
    server.init_server().await;
    println!("Server is listening on port: {}", server.port());

    let url = format!("http://localhost:{}", server.port());
    let resp = match reqwest::get(url).await {
        Ok(r) => r.text().await,
        Err(e) => Err(e),
    };
    match resp {
        Ok(r) => assert_eq!(true, r.starts_with("Try POST")),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}