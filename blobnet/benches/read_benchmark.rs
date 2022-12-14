use std::time::Duration;

use blobnet::provider::{self, Provider};
use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

async fn insert_read(provider: impl Provider) -> anyhow::Result<()> {
    let mut hashes = Vec::new();
    for i in 0..100 {
        let mut data = b"asdf".repeat(256);
        data.extend(u32::to_le_bytes(i));
        let hash = provider.put(Box::pin(&data[..])).await?;
        hashes.push(hash);
    }
    for _ in 0..100 {
        for hash in &hashes {
            provider.get(hash, None).await?;
        }
    }
    Ok(())
}

fn bench(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();

    c.bench_function("insert_read memory", |b| {
        b.to_async(&runtime)
            .iter(|| insert_read(provider::Memory::new()))
    });

    c.bench_function("insert_read localdir", |b| {
        let dir = tempfile::tempdir().unwrap();
        b.to_async(&runtime)
            .iter(|| insert_read(provider::LocalDir::new(dir.path())))
    });

    c.bench_function("insert_read cached localdir", |b| {
        let dir = tempfile::tempdir().unwrap();
        b.to_async(&runtime).iter(|| {
            let provider = provider::Cached::new(
                provider::LocalDir::new(dir.path().join("data")),
                dir.path().join("cache"),
                1 << 21,
            );
            insert_read(provider)
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(8));
    targets = bench
}
criterion_main!(benches);
