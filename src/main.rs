use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio_tungstenite::tungstenite::stream::Mode;

lazy_static::lazy_static! {
    static ref MODE_API_KEY: String = std::env::var("MODE_API_KEY")
        .expect("MODE_API_KEY must be set");
}

const API_INTERVAL: u64 = 1000;
const MAX_CONNECTIONS: usize = 500;

struct ModeApiRequest {
    miner: u64,
    asset: String,
    time_increment: u64,
    time_length: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_server().await
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {

    let API_REQUESTS: Vec<ModeApiRequest> = vec![
        ModeApiRequest {
            miner: 1,
            asset: "ETH".to_string(),
            time_increment: 30,
            time_length: 300,
        },
        ModeApiRequest {
            miner: 1,
            asset: "ETH".to_string(),
            time_increment: 2880,
            time_length: 28800,
        },
        ModeApiRequest {
            miner: 1,
            asset: "BTC".to_string(),
            time_increment: 30,
            time_length: 300,
        },
        ModeApiRequest {
            miner: 1,
            asset: "BTC".to_string(),
            time_increment: 2880,
            time_length: 28800,
        },
    ];

    let listener = TcpListener::bind("0.0.0.0:8080").await.expect("Failed to bind listener");
    println!("Websocket server listening on ws://0.0.0.0:8080");

    let (tx, _) = broadcast::channel::<String>(16);
    let tx_clone = tx.clone();

    // Track number of active subscribers
    let subscriber_count = Arc::new(AtomicUsize::new(0));
    let subscriber_count_api = subscriber_count.clone();

    let api_interval_handler = tokio::spawn(async move {
        let client = reqwest::Client::new();

        loop {
            // Only make API calls if there are active subscribers
            if subscriber_count_api.load(Ordering::Relaxed) > 0 {
                for (data) in API_REQUESTS.iter() {
                    match async {
                        let response = client.get("https://synth.mode.network/prediction/latest")
                            .query(&[
                                ("miner", data.miner.to_string()),
                                ("asset", data.asset.to_string()),
                                ("time_increment", data.time_increment.to_string()),
                                ("time_length", data.time_length.to_string())
                            ])
                            .header("accept", "application/json")
                            .header("Authorization", format!("Apikey {}",MODE_API_KEY.as_str()))
                            .send()
                            .await?;

                        let body: Value = response.json().await?;

                        Ok::<_, Box<dyn std::error::Error>>(serde_json::to_string(&body)?)
                    }.await {
                        Ok(message) => {
                            // Only send if we still have subscribers
                            if subscriber_count_api.load(Ordering::Relaxed) > 0 {
                                // Ignore send errors - this means no active subscribers
                                let _ = tx.send(message);
                            }
                        }
                        Err(e) => {
                            eprintln!("Error fetching/processing API data: {}", e);
                            // Implement exponential backoff or other error handling as needed
                        }
                    }
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(API_INTERVAL)).await;
        }
    });

    while let Ok((stream, addr)) = listener.accept().await {
        // Check connection limit
        if subscriber_count.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
            println!("Connection limit reached, rejecting client: {:?}", addr);
            continue;
        }

        println!("New client: {:?}", addr);
        let mut rx = tx_clone.subscribe();
        let subscriber_count = subscriber_count.clone();

        // Increment subscriber count
        subscriber_count.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let ws_stream = match accept_async(stream).await {
                Ok(ws_stream) => ws_stream,
                Err(e) => {
                    println!("Error during WebSocket handshake: {}", e);
                    subscriber_count.fetch_sub(1, Ordering::Relaxed);
                    return;
                }
            };

            let (mut write, read) = ws_stream.split();

            // Handle client disconnection
            let mut read = read.fuse();

            loop {
                tokio::select! {
                    message = rx.recv() => {
                        match message {
                            Ok(msg) => {
                                if let Err(e) = write.send(tokio_tungstenite::tungstenite::Message::Text(Utf8Bytes::from(msg))).await {
                                    println!("Error sending message to client: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                println!("Error receiving broadcast message: {}", e);
                                break;
                            }
                        }
                    }
                    // Handle client messages or disconnection
                    msg = read.next() => {
                        match msg {
                            Some(Ok(_)) => (), // Handle client messages if needed
                            Some(Err(e)) => {
                                println!("Error in client connection: {}", e);
                                break;
                            }
                            None => {
                                println!("Client disconnected");
                                break;
                            }
                        }
                    }
                }
            }

            // Decrement subscriber count on disconnect
            subscriber_count.fetch_sub(1, Ordering::Relaxed);
        });
    }

    api_interval_handler.await?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::time::Duration;
    use tokio::net::TcpStream;
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

    // Helper function to create a test client
    async fn create_test_client() -> WebSocketStream<MaybeTlsStream<TcpStream>> {
        let (ws_stream, _) = connect_async("ws://localhost:8080")
            .await
            .expect("Failed to connect");
        ws_stream
    }

    #[tokio::test]
    async fn test_websocket_connection() {
        // Start the server in a separate task
        let server_handle = tokio::spawn(async {
            run_server().await.expect("Error starting server");
        });

        // Give the server time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create a test client
        let ws_stream = create_test_client().await;
        let (_, mut read) = ws_stream.split();

        // Verify we can receive a message
        if let Some(msg) = read.next().await {
            assert!(msg.is_ok());
            let msg = msg.unwrap();
            assert!(msg.is_text());
        }

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_multiple_clients() {
        // Start server
        let server_handle = tokio::spawn(async {
            run_server().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create multiple clients
        let mut clients = vec![];
        for _ in 0..3 {
            let ws_stream = create_test_client().await;
            clients.push(ws_stream);
        }

        // Split streams and verify each client receives messages
        for mut client in clients {
            let (_, mut read) = client.split();

            if let Some(msg) = read.next().await {
                assert!(msg.is_ok());
                let msg = msg.unwrap();
                assert!(msg.is_text());

                // Verify message format
                let parsed: Value = serde_json::from_str::<_>(msg.to_text().unwrap()).unwrap();
                assert!(parsed.is_object());
            }
        }

        server_handle.abort();
    }
}