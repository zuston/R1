use log::{debug, error, info};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};

use crate::urpc::connection::Connection;
use crate::urpc::shutdown::Shutdown;

use crate::app::AppManagerRef;
use crate::await_tree::AWAIT_TREE_REGISTRY;
use crate::error::WorkerError;
use crate::metric::{URPC_CONNECTION_NUMBER, URPC_REQUEST_PROCESSING_LATENCY};
use crate::urpc::command::Command;
use anyhow::Result;
use await_tree::InstrumentAwait;
use tracing::Instrument;

const MAX_CONNECTIONS: usize = 40000;

struct Listener {
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl Listener {
    async fn run(&mut self, app_manager_ref: AppManagerRef) -> Result<()> {
        debug!("Accepting inbound connections");

        loop {
            let app_manager = app_manager_ref.clone();
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;
            let addr = socket.peer_addr()?.to_string();
            debug!("Accepted connection from client: {}", &addr);

            let mut handler = Handler {
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            let await_registry = AWAIT_TREE_REGISTRY.clone();
            let await_root = await_registry
                .register(format!("urpc connection with remote client: {}", addr))
                .await;
            tokio::spawn(await_root.instrument(async move {
                URPC_CONNECTION_NUMBER.inc();
                if let Err(error) = handler.run(app_manager).await {
                    error!("Errors on handling the request. {:#?}", error);
                }
                drop(permit);
                URPC_CONNECTION_NUMBER.dec();
            }));
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
            info!("Backoff: {}", backoff);
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

            let _ = URPC_REQUEST_PROCESSING_LATENCY
                .with_label_values(&[&format!("{}", &frame)])
                .start_timer();
            Command::from_frame(frame)?
                .apply(
                    app_manager_ref.clone(),
                    &mut self.connection,
                    &mut self.shutdown,
                )
                .instrument_await("handling the complete request")
                .await?;
        }
        Ok(())
    }
}

pub async fn run(listener: TcpListener, shutdown: impl Future, app_manager_ref: AppManagerRef) {
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
                error!("Errors on running urpc server. err: {:#?}", err);
            }
        }
        _ = shutdown => {
            info!("Accepting the shutdown signal for the urpc net service");
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
    use crate::app::AppManager;
    use crate::config::Config;
    use crate::config_reconfigure::ReconfigurableConfManager;
    use crate::decommission::DecommissionManager;
    use crate::rpc::DefaultRpcService;
    use crate::runtime::manager::RuntimeManager;
    use crate::storage::StorageService;
    use crate::urpc::frame::Frame;

    #[tokio::test]
    #[ignore]
    async fn rpc_start_test() -> anyhow::Result<()> {
        let mut config = Config::create_simple_config();
        config.grpc_port = 21100;
        config.urpc_port = Some(21101);

        let reconf_manager = ReconfigurableConfManager::new(&config, None)?;
        let runtime_manager = RuntimeManager::from(config.clone().runtime_config.clone());
        let storage = StorageService::init(&runtime_manager, &config);
        let app_manager_ref = AppManager::get_ref(
            runtime_manager.clone(),
            config.clone(),
            &storage,
            &reconf_manager,
        );

        DefaultRpcService {}.start(
            &config,
            runtime_manager.clone(),
            app_manager_ref.clone(),
            &DecommissionManager::new(&app_manager_ref),
        )?;

        Ok(())
    }

    #[test]
    fn enum_test() {
        let frame = Frame::GetLocalData(Default::default());
        assert_eq!("GetLocalData", format!("{}", frame));
    }
}
