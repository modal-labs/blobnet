use anyhow::Result;
use blobnet::client::FileClient;
use blobnet::server::{listen, Config};
use blobnet::{provider, ReadStream};
use hyper::client::HttpConnector;
use hyper::{body::Bytes, server::conn::AddrIncoming, Body};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

/// Spawn a temporary file server on localhost, only used for testing.
async fn spawn_temp_server() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let mut incoming = AddrIncoming::from_listener(listener)?;
    incoming.set_nodelay(true);
    tokio::spawn(async {
        let dir = tempdir().unwrap();
        let provider = provider::LocalDir::new(dir.path().join("nfs"));
        let provider = provider::Cached::new(provider, dir.path().join("cache"), 1 << 21);
        let config = Config {
            provider: Box::new(provider),
            secret: "secret".into(),
        };
        listen(config, incoming).await.unwrap();
    });
    Ok(format!("http://{addr}"))
}

fn http() -> HttpConnector {
    let mut conn = HttpConnector::new();
    conn.set_nodelay(true);
    conn
}

async fn eat(mut stream: ReadStream) -> Result<String> {
    let mut buf = String::new();
    stream.read_to_string(&mut buf).await?;
    Ok(buf)
}

#[tokio::test]
async fn single_file() -> Result<()> {
    let origin = spawn_temp_server().await?;
    let client = FileClient::new(http(), &origin, "secret");

    let s1 = "hello world!";
    let h1 = client.put(|| async { Ok(s1) }).await?;

    assert_eq!(eat(client.get(&h1, None).await?).await?, s1);
    assert_eq!(eat(client.get(&h1, Some((0, 2))).await?).await?, &s1[0..2]);
    Ok(())
}

#[tokio::test]
async fn missing_file() -> Result<()> {
    let origin = spawn_temp_server().await?;
    let client = FileClient::new(http(), &origin, "secret");

    assert!(client.get("not a valid sha-256 hash", None).await.is_err());

    let s1 = "my favorite poem";
    let hash = format!("{:x}", Sha256::new().chain_update(s1).finalize());
    assert!(client.get(&hash, None).await.is_err());
    assert_eq!(client.put(|| async { Ok(s1) }).await?, hash);
    assert_eq!(eat(client.get(&hash, None).await?).await?, s1);

    Ok(())
}

#[tokio::test]
async fn invalid_secret() -> Result<()> {
    let origin = spawn_temp_server().await?;
    let client = FileClient::new(http(), &origin, "wrong secret");

    let s1 = "hello world!";
    assert!(client.put(|| async { Ok(s1) }).await.is_err());
    Ok(())
}

#[tokio::test]
async fn large_50mb_stream() -> Result<()> {
    let origin = spawn_temp_server().await?;
    let client = FileClient::new(http(), &origin, "secret");

    let make_body = || async {
        let (mut sender, body) = Body::channel();
        tokio::spawn(async move {
            for ch in "abcde".chars() {
                let payload = Bytes::from(ch.to_string().repeat(1000000));
                for _ in 0..10 {
                    sender.send_data(payload.clone()).await.unwrap();
                }
            }
        });
        Ok(body)
    };
    let h1 = client.put(make_body).await?;

    assert_eq!(
        eat(client.get(&h1, Some((0, 100))).await?).await?,
        "a".repeat(100)
    );
    assert_eq!(
        eat(client.get(&h1, Some((9999999, 10000005))).await?).await?,
        "abbbbb"
    );
    assert_eq!(
        eat(client.get(&h1, Some((21239596, 21239600))).await?).await?,
        "cccc"
    );
    assert_eq!(
        eat(client.get(&h1, Some((49999996, 50000000))).await?).await?,
        "eeee"
    );
    assert!(client.get(&h1, Some((0, 50000001))).await.is_err());

    Ok(())
}
