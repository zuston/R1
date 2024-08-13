use log::{error, info};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::urpc::connection::Connection;
use crate::urpc::shutdown::Shutdown;

use crate::app::AppManagerRef;
use crate::error::WorkerError;
use crate::urpc::command::Command;
use anyhow::Result;

const MAX_CONNECTIONS: usize = 40000;

struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&mut self, app_manager_ref: AppManagerRef) -> Result<()> {
        info!("Accepting inbound connections");

        loop {
            let app_manager = app_manager_ref.clone();
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;
            let mut handler = Handler {
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(error) = handler.run(app_manager).await {
                    error!("Errors on handling the request. {:#?}", error);
                }
                drop(permit);
            });
        }
    }

    async fn accept(&mut self) -> Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff *= 2;
        }
    }
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    /// when the shutdown signal is received, the connection is processed
    /// util it reaches a safe state, at which point it is terminated
    async fn run(&mut self, app_manager_ref: AppManagerRef) -> Result<(), WorkerError> {
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                },
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            Command::from_frame(frame)?
                .apply(
                    app_manager_ref.clone(),
                    &mut self.connection,
                    &mut self.shutdown,
                )
                .await?;
        }
        Ok(())
    }
}

pub async fn urpc_serve(port: usize, shutdown: impl Future, app_manager_ref: AppManagerRef) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    run(listener, shutdown, app_manager_ref).await
}

async fn run(listener: TcpListener, shutdown: impl Future, app_manager_ref: AppManagerRef) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener {
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run(app_manager_ref) => {
            if let Err(err) = res {
                error!("Errors on running uproto server. err: {:#?}", err);
            }
        }
        _ = shutdown => {
            info!("Accepting the shutdown signal for the uproto net service");
        }
    }

    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

#[cfg(test)]
mod test {
    use crate::config::{
        Config, HybridStoreConfig, LocalfileStoreConfig, MemoryStoreConfig, MetricsConfig,
        StorageType,
    };
    use crate::start_uniffle_worker;
    use crate::urpc::server::run;
    use std::time::Duration;
    use tokio::net::TcpListener;

    async fn hang() -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_secs(1000000)).await;
        Ok(())
    }

    pub fn create_mocked_config(
        grpc_port: i32,
        capacity: String,
        local_data_path: String,
    ) -> Config {
        Config {
            memory_store: Some(MemoryStoreConfig::new(capacity)),
            localfile_store: Some(LocalfileStoreConfig {
                data_paths: vec![local_data_path],
                healthy_check_min_disks: Some(0),
                disk_high_watermark: None,
                disk_low_watermark: None,
                disk_max_concurrency: None,
            }),
            hybrid_store: Some(HybridStoreConfig::new(0.9, 0.5, None)),
            hdfs_store: None,
            store_type: Some(StorageType::MEMORY_LOCALFILE),
            runtime_config: Default::default(),
            metrics: Some(MetricsConfig {
                push_gateway_endpoint: None,
                push_interval_sec: None,
            }),
            grpc_port: Some(grpc_port),
            uprc_port: None,
            coordinator_quorum: vec![],
            tags: None,
            log: None,
            app_heartbeat_timeout_min: None,
            huge_partition_marked_threshold: None,
            huge_partition_memory_max_used_percent: None,
            http_monitor_service_port: None,
            tracing: None,
        }
    }

    #[tokio::test]
    async fn test() -> anyhow::Result<()> {
        let temp_dir = tempdir::TempDir::new("test_write_read").unwrap();
        let temp_path = temp_dir.path().to_str().unwrap().to_string();
        println!("created the temp file path: {}", &temp_path);

        let config = create_mocked_config(20001, "10M".to_string(), temp_path);
        let app_ref = start_uniffle_worker(config).await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:19991").await.unwrap();
        run(listener, hang(), app_ref).await;
        Ok(())
    }
}