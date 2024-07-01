use {
    anyhow::{Context, Result}, solana_storage_bigtable::{CredentialType, LedgerStorageConfig}
};

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct Config {
    pub bigtable: LedgerStorageConfig,
    pub db_url: String,
}

impl Config {
    pub async fn new() -> Self { Self::default() }
    pub async fn load(path: &str) -> Result<Self> {
        serde_yaml::from_str(
            &tokio::fs::read_to_string(path).await?
        ).with_context(|| "failed to deserialize config")
    }
    pub fn bigtable_credentials_path(
        &mut self,
        path: &str
    ) {
        self.bigtable.credential_type = CredentialType::Filepath(Some(path.to_string()))
    }
    pub async fn save(&self, path: &str) -> Result<()> {
        tokio::fs::write(
            path,
            serde_yaml::to_string(self).with_context(|| "failed to serialize config")?
        ).await.with_context(|| "failed to write config")
    }
}