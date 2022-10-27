//! This is a quick benchmark for testing the latency of a single repeated read
//! from blobnet.
//!
//! To use this script, run like the following example:
//!
//! ```bash
//! cargo run --release --example speed http://127.0.0.1:7609 secret
//! ```
//!
//! The first argument is the address of the blobnet server, and the second
//! argument is the hash of a file to retrieve from the server.
//!
//! The third argument is the authentication secret.

use std::{env, time::Instant};

use anyhow::Result;
use blobnet::{client::FileClient, read_to_vec};
use hyper::client::HttpConnector;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    assert!(args.len() == 3, "usage: {} <origin> <secret>", args[0]);

    let origin = &args[1];
    let secret = &args[2];

    let data = str::repeat("abcdefghijklmnop", 4096); // 64 KiB

    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    let client = FileClient::new(connector, origin, secret);
    let hash = client.put(|| async { Ok(data.clone()) }).await?;

    let output = read_to_vec(client.get(&hash, None).await?).await?;
    println!("read {} bytes", output.len());

    let mut times = vec![];
    for _ in 0..10000 {
        let start = Instant::now();
        let output2 = read_to_vec(client.get(&hash, None).await?).await?;
        times.push(start.elapsed().as_micros());
        assert!(output2.len() == output.len());
    }
    println!("avg = {} us", times.iter().sum::<u128>() as f64 / 10000.0);
    Ok(())
}
