use crate::metric::{GAUGE_GRPC_REQUEST_QUEUE_SIZE, TOTAL_GRPC_REQUEST};
use hyper::service::Service;
use hyper::Body;
use prometheus::HistogramVec;
use std::task::{Context, Poll};
use tower::Layer;

#[derive(Clone)]
pub struct MetricsMiddlewareLayer {
    metric: HistogramVec,
}

impl MetricsMiddlewareLayer {
    pub fn new(metric: HistogramVec) -> Self {
        Self { metric }
    }
}

impl<S> Layer<S> for MetricsMiddlewareLayer {
    type Service = MetricsMiddleware<S>;

    fn layer(&self, service: S) -> Self::Service {
        MetricsMiddleware {
            inner: service,
            metric: self.metric.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricsMiddleware<S> {
    inner: S,
    metric: HistogramVec,
}

impl<S> Service<hyper::Request<Body>> for MetricsMiddleware<S>
where
    S: Service<hyper::Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
        TOTAL_GRPC_REQUEST.with_label_values(&[&"ALL"]).inc();
        GAUGE_GRPC_REQUEST_QUEUE_SIZE.inc();

        // This is necessary because tonic internally uses `tower::buffer::Buffer`.
        // See https://github.com/tower-rs/tower/issues/547#issuecomment-767629149
        // for details on why this is necessary
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let metrics = self.metric.clone();

        Box::pin(async move {
            let path = req.uri().path();
            TOTAL_GRPC_REQUEST.with_label_values(&[path]).inc();
            let timer = metrics.with_label_values(&[path]).start_timer();

            let response = inner.call(req).await?;

            timer.observe_duration();

            GAUGE_GRPC_REQUEST_QUEUE_SIZE.dec();

            Ok(response)
        })
    }
}
