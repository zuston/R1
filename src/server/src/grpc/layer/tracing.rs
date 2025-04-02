use fastrace::collector::SpanContext;
use fastrace::future::FutureExt;
use fastrace::Span;
use hyper::Body;
use std::future::Future;
use std::task::{Context, Poll};
use tower::layer::util::Identity;
use tower::util::Either;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct TracingMiddleWareLayer {}

#[derive(Clone)]
pub struct TracingMiddleWare<S> {
    inner: S,
}

pub type OptionalAwaitTreeMiddlewareLayer = Either<TracingMiddleWareLayer, Identity>;

impl TracingMiddleWareLayer {
    pub fn new() -> OptionalAwaitTreeMiddlewareLayer {
        Either::A(Self {})
    }
}

impl<S> Layer<S> for TracingMiddleWareLayer {
    type Service = TracingMiddleWare<S>;

    fn layer(&self, service: S) -> Self::Service {
        TracingMiddleWare { inner: service }
    }
}

impl<S> Service<hyper::Request<Body>> for TracingMiddleWare<S>
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

        let uri: String = req.uri().path().into();
        let parent = SpanContext::random();
        let span = Span::root(uri, parent);

        async move { inner.call(req).await }.in_span(span)
    }
}
