use thiserror::Error;

use crate::model::{AnchorSnapshot, PulseAsset};

pub type Result<T> = std::result::Result<T, AnchorError>;

pub trait AnchorProvider: Send + Sync {
    fn provider_id(&self) -> &str;
    fn snapshot_for(&self, asset: PulseAsset) -> Result<Option<AnchorSnapshot>> {
        self.snapshot_for_target(asset, None)
    }

    fn snapshot_for_target(
        &self,
        asset: PulseAsset,
        target_event_expiry_ts_ms: Option<u64>,
    ) -> Result<Option<AnchorSnapshot>>;
}

#[derive(Debug, Error)]
pub enum AnchorError {
    #[error("unsupported pulse asset `{asset}`")]
    UnsupportedAsset { asset: String },
    #[error("unsupported anchor provider kind `{kind}` for `{provider_id}`")]
    UnsupportedProviderKind { provider_id: String, kind: String },
    #[error("missing anchor provider `{provider_id}`")]
    MissingProvider { provider_id: String },
    #[error("anchor provider `{provider_id}` is disabled")]
    ProviderDisabled { provider_id: String },
    #[error("no anchor provider routed for asset `{asset}`")]
    MissingRoute { asset: String },
    #[error("anchor snapshot is missing required field: {0}")]
    InvalidSnapshot(String),
}
