use {
    anyhow::{Context, Result},
    std::time::Duration,
};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    pub bigtable: BigTableConfig,
    pub db_url: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BigTableConfig {
    pub credentials_file: String,
    pub project_id: String,
    pub instance_name: String,
    pub channel_size: usize,
    pub timeout: Duration,
    // max decoding size in mb
    pub max_decoding_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bigtable: Default::default(),
            db_url: "postgres://postgres:password123@localhost/sbdl".to_string(),
        }
    }
}

impl Default for BigTableConfig {
    fn default() -> Self {
        Self {
            credentials_file: "".to_string(),
            project_id: "mainnet-beta".to_string(),
            instance_name: "solana-ledger".to_string(),
            channel_size: 10,
            timeout: Duration::from_secs(10),
            max_decoding_size: 100*1024*1024
        }
    }
}

impl Config {
    pub async fn new() -> Self {
        Self::default()
    }
    pub async fn load(path: &str) -> Result<Self> {
        serde_yaml::from_str(&tokio::fs::read_to_string(path).await?)
            .with_context(|| "failed to deserialize config")
    }
    pub async fn save(&self, path: &str) -> Result<()> {
        tokio::fs::write(
            path,
            serde_yaml::to_string(self).with_context(|| "failed to serialize config")?,
        )
        .await
        .with_context(|| "failed to write config")
    }
}
