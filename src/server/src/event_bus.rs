use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::metric::{
    EVENT_BUS_HANDLE_DURATION, GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE,
    GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE, TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE,
    TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE,
};
use crate::runtime::RuntimeRef;
use async_trait::async_trait;
use await_tree::InstrumentAwait;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::Instrument;

#[async_trait]
pub trait Subscriber: Send + Sync {
    type Input;

    async fn on_event(&self, event: Event<Self::Input>) -> bool;
}

#[derive(Debug, Clone)]
pub struct Event<T> {
    pub data: T,
}

impl<T: Send + Sync + Clone> Event<T> {
    pub fn new(data: T) -> Event<T> {
        Event { data }
    }

    pub fn get_data(&self) -> &T {
        &self.data
    }
}

impl<T: Send + Sync + Clone> From<T> for Event<T> {
    fn from(data: T) -> Self {
        Event::new(data)
    }
}

#[derive(Clone)]
pub struct EventBus<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    subscriber: OnceCell<Arc<Box<dyn Subscriber<Input = T> + 'static>>>,

    /// Using the async_channel to keep the immutable self to
    /// the self as the Arc<xxx> rather than mpsc::channel, which
    /// uses the recv(&mut self). I don't hope so.
    queue_recv: async_channel::Receiver<Event<T>>,
    queue_send: async_channel::Sender<Event<T>>,

    name: String,
    runtime: RuntimeRef,

    concurrency_num: usize,
    concurrency_limit: Arc<Semaphore>,
}

unsafe impl<T: Send + Sync + 'static> Send for EventBus<T> {}
unsafe impl<T: Send + Sync + 'static> Sync for EventBus<T> {}

impl<T: Send + Sync + Clone + 'static> EventBus<T> {
    pub fn new(runtime: &RuntimeRef, name: String, concurrency_limit: usize) -> EventBus<T> {
        let runtime = runtime.clone();

        let (send, recv) = async_channel::unbounded();
        let concurrency_limiter = Arc::new(Semaphore::new(concurrency_limit));
        let event_bus = EventBus {
            inner: Arc::new(Inner {
                subscriber: OnceCell::new(),
                queue_recv: recv,
                queue_send: send,
                name: name.to_string(),
                runtime: runtime.clone(),
                concurrency_num: concurrency_limit,
                concurrency_limit: concurrency_limiter,
            }),
        };

        let cloned = event_bus.clone();
        runtime.spawn_with_await_tree(format!("EventBus - [{}]", &name).as_str(), async move {
            EventBus::handle(cloned).await;
        });

        event_bus
    }

    async fn handle(event_bus: EventBus<T>) {
        while let Ok(message) = event_bus
            .inner
            .queue_recv
            .recv()
            .instrument_await("receiving event")
            .await
        {
            let concurrency_guarder = event_bus
                .inner
                .concurrency_limit
                .clone()
                .acquire_owned()
                .instrument_await("waiting for the spill concurrent reject.")
                .await
                .unwrap();

            let bus = event_bus.clone();
            event_bus.inner.runtime.spawn_with_await_tree(
                format!("EventBus - [{}] - Handler", &event_bus.inner.name).as_str(),
                async move {
                    let timer = EVENT_BUS_HANDLE_DURATION
                        .with_label_values(&[&bus.inner.name])
                        .start_timer();
                    GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE
                        .with_label_values(&[&bus.inner.name])
                        .inc();
                    GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
                        .with_label_values(&[&bus.inner.name])
                        .dec();

                    let binding = bus.inner.subscriber.get();
                    let subscriber = binding.as_ref().unwrap();
                    let _ = subscriber.on_event(message).await;

                    timer.observe_duration();
                    GAUGE_EVENT_BUS_QUEUE_HANDLING_SIZE
                        .with_label_values(&[&bus.inner.name])
                        .dec();
                    TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE
                        .with_label_values(&[&bus.inner.name])
                        .inc();

                    drop(concurrency_guarder);
                },
            );
        }
    }

    pub fn subscribe<R: Subscriber<Input = T> + 'static + Send + Sync>(&self, listener: R) {
        let _ = self.inner.subscriber.set(Arc::new(Box::new(listener)));
    }

    pub async fn publish(&self, event: Event<T>) -> anyhow::Result<()> {
        self.inner.queue_send.send(event).await?;

        GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
            .with_label_values(&[&self.inner.name])
            .inc();
        TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE
            .with_label_values(&[&self.inner.name])
            .inc();
        Ok(())
    }

    pub fn sync_publish(&self, event: Event<T>) -> anyhow::Result<()> {
        self.inner.queue_send.send_blocking(event)?;

        GAUGE_EVENT_BUS_QUEUE_PENDING_SIZE
            .with_label_values(&[&self.inner.name])
            .inc();
        TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE
            .with_label_values(&[&self.inner.name])
            .inc();
        Ok(())
    }

    pub fn concurrency_limit(&self) -> usize {
        self.inner.concurrency_num
    }
}

#[cfg(test)]
mod test {
    use crate::event_bus::{Event, EventBus, Subscriber};
    use crate::metric::{TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE, TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE};
    use crate::runtime::manager::create_runtime;
    use async_trait::async_trait;
    use std::sync::atomic::Ordering::{Relaxed, SeqCst};
    use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_event_bus() -> anyhow::Result<()> {
        let runtime = create_runtime(1, "test");
        let mut event_bus = EventBus::new(&runtime, "test".to_string(), 1usize);
        let flag = Arc::new(AtomicI64::new(0));

        struct SimpleCallback {
            flag: Arc<AtomicI64>,
        }

        #[async_trait]
        impl Subscriber for SimpleCallback {
            type Input = String;

            async fn on_event(&self, event: Event<Self::Input>) -> bool {
                println!("SimpleCallback has accepted event: {:?}", event.get_data());
                self.flag.fetch_add(1, Ordering::SeqCst);
                true
            }
        }

        let flag_cloned = flag.clone();
        event_bus.subscribe(SimpleCallback { flag: flag_cloned });

        let bus = event_bus.clone();
        let _ =
            runtime.block_on(async move { bus.publish("singleEvent".to_string().into()).await });

        // case1: check the handle logic
        awaitility::at_most(Duration::from_secs(1)).until(|| flag.load(Ordering::SeqCst) == 1);

        // case2: check the metrics
        assert_eq!(
            1,
            TOTAL_EVENT_BUS_EVENT_HANDLED_SIZE
                .with_label_values(&["test"])
                .get()
        );
        assert_eq!(
            1,
            TOTAL_EVENT_BUS_EVENT_PUBLISHED_SIZE
                .with_label_values(&["test"])
                .get()
        );

        Ok(())
    }
}
