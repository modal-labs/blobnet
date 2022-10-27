# blobnet

[![Latest Version](https://img.shields.io/crates/v/blobnet.svg)](https://crates.io/crates/blobnet)
[![API Documentation](https://docs.rs/blobnet/badge.svg)](https://docs.rs/blobnet)

A configurable, low-latency blob storage server for content-addressed data.

This acts as a non-volatile, over-the-network content cache. Internal users can
add binary blobs to the cache, and the data is indexed by its SHA-256 hash. Any
blob can be retrieved by its hash and the range of bytes to read.

Data stored in blobnet is locally cached and durable.

## Usage

Run `cargo install blobnet` and see the server options through a CLI. The server
supports only three types of requests (`HEAD` / `GET` / `PUT`), and a full
client implementation, including optional secondary local caching, is in the
`blobnet` library crate.
