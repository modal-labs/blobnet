//! Property-based consistency tests for blobnet.

use std::path::Path;

use blobnet::{
    provider::{Cached, LocalDir, Memory, Provider},
    read_to_vec, Error,
};
use quickcheck::quickcheck;

quickcheck! {
    fn cached_comparison(data: Vec<u8>, range: Option<(u64, u64)>) -> Result<bool, Error> {
        let dir = tempfile::tempdir()?;
        run_cached_comparison(dir.path(), data, range)
    }
}

#[tokio::main(flavor = "current_thread")]
async fn run_cached_comparison(
    dir: &Path,
    data: Vec<u8>,
    mut range: Option<(u64, u64)>,
) -> Result<bool, Error> {
    let data = data.repeat(100);

    if let Some(range) = &mut range {
        range.0 %= data.len() as u64 + 5;
        range.1 %= data.len() as u64 + 5;
        if range.0 > range.1 {
            std::mem::swap(&mut range.0, &mut range.1);
        }
    }

    let mem = Memory::new();
    let ldir = LocalDir::new(dir.join("ldir"));
    let mem_cached = Cached::new(Memory::new(), dir.join("c1"), 4096);
    let ldir_cached = Cached::new(LocalDir::new(dir.join("ldir2")), dir.join("c2"), 4096);

    let h1 = mem.put(Box::pin(&data[..])).await?;
    let h2 = ldir.put(Box::pin(&data[..])).await?;
    let h3 = mem_cached.put(Box::pin(&data[..])).await?;
    let h4 = ldir_cached.put(Box::pin(&data[..])).await?;

    Ok(h1 == h2 && h1 == h3 && h1 == h4 && {
        let r1 = mem.get(&h1, range).await;
        let r2 = ldir.get(&h1, range).await;
        let r3 = mem_cached.get(&h1, range).await;
        let r4 = ldir_cached.get(&h1, range).await;

        if r1.is_ok() {
            let c1 = read_to_vec(r1?).await?;
            let c2 = read_to_vec(r2?).await?;
            let c3 = read_to_vec(r3?).await?;
            let c4 = read_to_vec(r4?).await?;
            c1 == c2 && c1 == c3 && c1 == c4
        } else {
            r2.is_err() && r3.is_err() && r4.is_err()
        }
    })
}
