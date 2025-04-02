use crate::app::{SHUFFLE_SERVER_ID, SHUFFLE_SERVER_IP};
use crate::config::Config;
use crate::util::{generate_worker_uid, get_local_ip};

pub fn init_global_variable(config: &Config) {
    let worker_uid = generate_worker_uid(&config);
    SHUFFLE_SERVER_ID.get_or_init(|| worker_uid.clone());

    let worker_ip = get_local_ip().unwrap().to_string();
    SHUFFLE_SERVER_IP.get_or_init(|| worker_ip);
}
