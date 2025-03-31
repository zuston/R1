mod datasource;

#[cfg(test)]
mod tests {
    use crate::admin::datasource::Datasource;
    use crate::discovery::InstanceInfo;
    use anyhow::Result;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_datasource() -> Result<()> {
        let mut ctx = SessionContext::new();

        let instances = vec![
            InstanceInfo {
                service_type: "riffle_server".to_string(),
                ip: "10.8.9.10".to_string(),
                grpc_port: 20010,
                hostname: "host.xx.xx".to_string(),
                version: "0.8.2-rc1".to_string(),
                cluster: "ali-1".to_string(),
            },
            InstanceInfo {
                service_type: "riffle_server".to_string(),
                ip: "10.8.9.11".to_string(),
                grpc_port: 20010,
                hostname: "host.xx.xx".to_string(),
                version: "0.8.2-rc1".to_string(),
                cluster: "ali-2".to_string(),
            },
        ];
        let store_path = tempfile::tempdir()?;
        Datasource::register(&ctx, &store_path, instances, "riffle_instances").await?;
        let sql = "SELECT * FROM riffle_instances";
        let df = ctx.sql(sql).await?;
        df.show().await?;

        Ok(())
    }
}
