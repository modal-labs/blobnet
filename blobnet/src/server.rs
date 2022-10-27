//! Server that makes blobnet accessible over HTTP.

use std::convert::Infallible;
use std::error::Error as StdError;
use std::future::{self, Future};
use std::sync::Arc;

use hyper::header::HeaderValue;
use hyper::server::accept::Accept;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::headers::{HEADER_FILE_LENGTH, HEADER_RANGE, HEADER_SECRET};
use crate::provider::Provider;
use crate::utils::{body_stream, chunked_body, get_hash};

/// Configuration for the file server.
pub struct Config {
    /// Provider that is used to handle requests.
    pub provider: Box<dyn Provider>,

    /// Secret that authorizes users to access the service.
    pub secret: String,
}

/// Create a file server listening at the given address.
pub async fn listen<I>(config: Config, incoming: I) -> hyper::Result<()>
where
    I: Accept,
    I::Error: Into<Box<dyn StdError + Send + Sync>>,
    I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    listen_with_shutdown(config, incoming, future::pending()).await
}

/// Create a file server listening at the given address, with graceful shutdown.
pub async fn listen_with_shutdown<I>(
    config: Config,
    incoming: I,
    shutdown: impl Future<Output = ()>,
) -> hyper::Result<()>
where
    I: Accept,
    I::Error: Into<Box<dyn StdError + Send + Sync>>,
    I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let config = Arc::new(config);

    // Low-level service boilerplate to interface with the [`hyper`] API.
    let make_svc = make_service_fn(move |_conn| {
        let config = Arc::clone(&config);
        async {
            Ok::<_, Infallible>(service_fn(move |req| {
                let config = Arc::clone(&config);
                async {
                    let resp = handle(config, req).await;
                    Ok::<_, Infallible>(resp.unwrap_or_else(|code| {
                        Response::builder()
                            .status(code)
                            .body(Body::empty())
                            .unwrap()
                    }))
                }
            }))
        }
    });

    Server::builder(incoming)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown)
        .await
}

const E500: StatusCode = StatusCode::INTERNAL_SERVER_ERROR;

/// Main service handler for the file server.
async fn handle(config: Arc<Config>, req: Request<Body>) -> Result<Response<Body>, StatusCode> {
    let secret = req.headers().get(HEADER_SECRET);
    let secret = secret.and_then(|s| s.to_str().ok());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => Ok(Response::new(Body::from("blobnet ok\n"))),
        _ if secret != Some(&config.secret) => Err(StatusCode::UNAUTHORIZED),
        (&Method::HEAD, path) => {
            let hash = get_hash(path)?;
            let len = config.provider.head(hash).await?;
            Response::builder()
                .header(HEADER_FILE_LENGTH, len.to_string())
                .body(Body::empty())
                .map_err(|_| E500)
        }
        (&Method::GET, path) => {
            let range = req.headers().get(HEADER_RANGE).and_then(parse_range_header);
            println!("server range header: {:?}", range);
            let hash = get_hash(path)?;
            let reader = config.provider.get(hash, range).await?;
            Ok(Response::new(chunked_body(reader)))
        }
        (&Method::PUT, "/") => {
            let body = req.into_body();
            let hash = config.provider.put(body_stream(body)).await?;
            Ok(Response::new(Body::from(hash)))
        }
        _ => Err(StatusCode::NOT_FOUND),
    }
}

/// Parse an HTTP Range header of the format "X-Bn-Range: <start>-<end>".
///
/// The start index is inclusive, and the end index is exclusive. This differs
/// from the standard HTTP `Range` header, which has both ends inclusive.
fn parse_range_header(s: &HeaderValue) -> Option<(u64, u64)> {
    let s = s.to_str().ok()?;
    let (start, end) = s.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}
