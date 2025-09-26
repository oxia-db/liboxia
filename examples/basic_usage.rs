use oxia_client_rust::client::Client;
use oxia_client_rust::client::PutOptions;
use oxia_client_rust::client_builder::OxiaClientBuilder;
#[tokio::main]
async fn main() {
    let client = OxiaClientBuilder::new().build().await.unwrap();
    let result = client
        .put(String::from("key"), Vec::new(), PutOptions {})
        .await
        .unwrap();
    ()
}
