Another high-performance implementation of Apache Uniffle shuffle server

> Production ready, this project has been used for online shuffle storage service

<details>

<summary>Benchmark report</summary>

#### Environment

| type                | description                                                             |
|---------------------|:------------------------------------------------------------------------|
| Software            | Uniffle 0.8.0 / Hadoop 3.2.2 / Spark 3.1.2                              |
| Hardware            | Machine 96 cores, 512G memory, 1T * 4 SATA SSD, network bandwidth 8GB/s |
| Hadoop Yarn Cluster | 1 * ResourceManager + 40 * NodeManager, every machine 1T * 4 SATA SSD   |
| Uniffle Cluster     | 1 * Coordinator + 1 * Shuffle Server, every machine 1T * 4 SATA SSD     |

#### Configuration

__spark's conf__
```yaml
spark.executor.instances 400
spark.executor.cores 1
spark.executor.memory 2g
spark.shuffle.manager org.apache.spark.shuffle.RssShuffleManager
spark.rss.storage.type MEMORY_LOCALFILE
```

__Rust-based shuffle-server conf__
```
store_type = "MEMORY_LOCALFILE"
grpc_port = 21100
coordinator_quorum = ["xxxxx:21000"]
tags = ["riffle2", "datanode", "GRPC", "ss_v5"]

[memory_store]
capacity = "10G"
dashmap_shard_amount = 128

[localfile_store]
data_paths = ["/data1/uniffle/t1", "/data2/uniffle/t1", "/data3/uniffle/t1", "/data4/uniffle/t1"]
healthy_check_min_disks = 0
disk_max_concurrency = 2000

[hybrid_store]
memory_spill_high_watermark = 0.5
memory_spill_low_watermark = 0.2
memory_spill_max_concurrency = 1000

[metrics]
http_port = 19998
push_gateway_endpoint = "http://xxxxx/prometheus/pushgateway"

[runtime_config]
read_thread_num = 40
write_thread_num = 200
grpc_thread_num = 100
http_thread_num = 10
default_thread_num = 20
dispatch_thread_num = 10
```
`GRPC_PARALLELISM=100 WORKER_IP=10.0.0.1 RUST_LOG=info ./uniffle-worker`

#### TeraSort Result

| type/buffer capacity                  | 273G (compressed)  |
|---------------------------------------|:------------------:|
| vanilla spark ESS                     | 4.2min (1.3m/2.9m) |
|                                       |                    |
| riffle(grpc) / 10g                    | 4.0min (1.9m/2.1m) |
| riffle(grpc) / 300g                   | 3.5min (1.4m/2.1m) |
|                                       |                    |
| riffle(urpc) / 10g                    | 3.8min (1.6m/2.2m) |
| riffle(urpc) / 300g                   | 3.2min (1.2m/2.0m) |
|                                       |                    |
| uniffle(grpc)/ 10g                    | 4.0min (1.8m/2.2m) |
| uniffle(grpc)/ 300g                   | 8.6min (2.7m/5.9m) |
|                                       |                    |
| uniffle(netty)(default malloc) 10g    | 5.1min (2.7m/2.4m) |
| uniffle(netty)(jemalloc) 10g          | 4.5min (2.0m/2.5m) |
| uniffle(netty)(default malloc)/ 300g  | 4.0min (1.5m/2.5m) |
| uniffle(netty)(jemalloc)/ 300g        | 6.6min (1.9m/4.7m) |

> tips: the riffle's urpc implements the customized tcp stream's proto, that is named with the NETTY rpc type in java side. 
 
</details>

## Build

`cargo build --release --features hdfs,jemalloc`

Uniffle-x currently treats all compiler warnings as error, with some dead-code warning excluded. When you are developing
and really want to ignore the warnings for now, you can use `cargo --config 'build.rustflags=["-W", "warnings"]' build`
to restore the default behavior. However, before submit your pr, you should fix all the warnings.

## Run

`WORKER_IP={ip} RUST_LOG=info WORKER_CONFIG_PATH=./config.toml ./uniffle-worker`

### HDFS Setup

Benefit from the hdfs-native crate, there is no need to setup the JAVA_HOME and relative dependencies.
If HDFS store is valid, the spark client must specify the conf of `spark.rss.client.remote.storage.useLocalConfAsDefault=true`

```shell
cargo build --features hdfs --release
```

```shell
# configure the kerberos
KRB5_CONFIG=/etc/krb5.conf KRB5CCNAME=/tmp/krb5cc_2002 LOG=info ./uniffle-worker
```


<details>

<summary>All config options</summary>

```toml
store_type = "MEMORY_LOCALFILE_HDFS"
grpc_port = 19999
coordinator_quorum = ["host1:port", "host2:port"]
urpc_port = 20000
http_monitor_service_port = 20010
heartbeat_interval_seconds = 2
tags = ["GRPC", "ss_v5", "GRPC_NETTY"]

[memory_store]
capacity = "1G"
buffer_ticket_timeout_sec = 300
buffer_ticket_check_interval_sec = 10
dashmap_shard_amount = 128

[localfile_store]
data_paths = ["/var/data/path1", "/var/data/path2"]
min_number_of_available_disks = 1
disk_high_watermark = 0.8
disk_low_watermark = 0.7
disk_max_concurrency = 2000
disk_write_buf_capacity = "1M"
disk_read_buf_capacity = "1M"
disk_healthy_check_interval_sec = 60

[hdfs_store]
max_concurrency = 50
partition_write_max_concurrency = 20

[hdfs_store.kerberos_security_config]
keytab_path = "/path/to/keytab"
principal = "principal@REALM"

[hybrid_store]
memory_spill_high_watermark = 0.8
memory_spill_low_watermark = 0.2
memory_single_buffer_max_spill_size = "1G"
memory_spill_to_cold_threshold_size = "128M"
memory_spill_to_localfile_concurrency = 4000
memory_spill_to_hdfs_concurrency = 500
huge_partition_memory_spill_to_hdfs_threshold_size = "64M"

[runtime_config]
read_thread_num = 100
localfile_write_thread_num = 100
hdfs_write_thread_num = 20
http_thread_num = 2
default_thread_num = 10
dispatch_thread_num = 100

[metrics]
push_gateway_endpoint = "http://example.com/metrics"
push_interval_sec = 10
labels = { env = "production", service = "my_service" }

[log]
path = "/var/log/my_service.log"
rotation = "Daily"

[app_config]
app_heartbeat_timeout_min = 5
huge_partition_marked_threshold = "1G"
huge_partition_memory_limit_percent = 0.75

[tracing]
jaeger_reporter_endpoint = "http://jaeger:14268"
jaeger_service_name = "my_service"

[health_service_config]
alive_app_number_max_limit = 100
```

</details>

## Profiling

### Heap profiling
1. build with profile support
    ```shell
    cargo build --release --features memory-prof
    ```
2. Start with profile
    ```shell
    curl localhost:20010/debug/heap/profile > heap.pb.gz
    go tool pprof -http="0.0.0.0:8081" heap.pb.gz
    ```
   
### CPU Profiling
1. build with jemalloc feature
    ```shell
    cargo build --release --features jemalloc
    ```
2. Paste following command to get cpu profile flamegraph
    ```shell
    go tool pprof -http="0.0.0.0:8081" http://{remote_ip}:8080/debug/pprof/profile?seconds=30
    ```
   - localhost:8080: riffle server.
   - remote_ip: pprof server address.
   - seconds=30: Profiling lasts for 30 seconds.

   Then open the URL <your-ip>:8081/ui/flamegraph in your browser to view the flamegraph:
