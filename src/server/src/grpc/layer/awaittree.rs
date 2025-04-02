use std::task::{Context, Poll};

use crate::await_tree::AwaitTreeInner;
use futures::Future;
use hyper::Body;
use tower::layer::util::Identity;
use tower::util::Either;
use tower::{Layer, Service};

/// Manages the await-trees of `gRPC` requests that are currently served by the compute node.

#[derive(Clone)]
pub struct AwaitTreeMiddlewareLayer {
    manager: AwaitTreeInner,
}
pub type OptionalAwaitTreeMiddlewareLayer = Either<AwaitTreeMiddlewareLayer, Identity>;

impl AwaitTreeMiddlewareLayer {
    pub fn new(manager: AwaitTreeInner) -> Self {
        Self { manager }
    }

    pub fn new_optional(optional: Option<AwaitTreeInner>) -> OptionalAwaitTreeMiddlewareLayer {
        if let Some(manager) = optional {
            Either::A(Self::new(manager))
        } else {
            Either::B(Identity::new())
        }
    }
}

impl<S> Layer<S> for AwaitTreeMiddlewareLayer {
    type Service = AwaitTreeMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        AwaitTreeMiddleware {
            inner: service,
            manager: self.manager.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AwaitTreeMiddleware<S> {
    inner: S,
    manager: AwaitTreeInner,
}

impl<S> Service<hyper::Request<Body>> for AwaitTreeMiddleware<S>
where
    S: Service<hyper::Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let manager = self.manager.clone();
        async move {
            let root = manager.register(format!("{}", req.uri().path())).await;
            root.instrument(inner.call(req)).await
        }
    }
}
