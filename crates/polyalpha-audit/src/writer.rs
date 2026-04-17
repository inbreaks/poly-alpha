use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::model::{
    AuditCheckpoint, AuditEvent, AuditSessionManifest, AuditSessionSummary, NewAuditEvent,
    PulseExecutionContext,
};

#[derive(Clone, Debug)]
pub struct AuditPaths {
    root: PathBuf,
    session_dir: PathBuf,
    raw_dir: PathBuf,
    manifest_path: PathBuf,
    summary_path: PathBuf,
    checkpoint_path: PathBuf,
    execution_context_path: PathBuf,
    warehouse_dir: PathBuf,
}

impl AuditPaths {
    pub fn new(root: impl AsRef<Path>, session_id: &str) -> Self {
        let audit_root = root.as_ref().join("audit");
        let session_dir = audit_root.join("sessions").join(session_id);
        Self {
            root: audit_root.clone(),
            session_dir: session_dir.clone(),
            raw_dir: session_dir.join("raw"),
            manifest_path: session_dir.join("manifest.json"),
            summary_path: session_dir.join("summary.json"),
            checkpoint_path: session_dir.join("checkpoint.json"),
            execution_context_path: session_dir.join("execution_context.json"),
            warehouse_dir: audit_root.join("warehouse"),
        }
    }

    pub fn audit_root(&self) -> &Path {
        &self.root
    }

    pub fn session_dir(&self) -> &Path {
        &self.session_dir
    }

    pub fn raw_dir(&self) -> &Path {
        &self.raw_dir
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn summary_path(&self) -> &Path {
        &self.summary_path
    }

    pub fn checkpoint_path(&self) -> &Path {
        &self.checkpoint_path
    }

    pub fn execution_context_path(&self) -> &Path {
        &self.execution_context_path
    }

    pub fn warehouse_dir(&self) -> &Path {
        &self.warehouse_dir
    }

    pub fn sessions_root(root: impl AsRef<Path>) -> PathBuf {
        root.as_ref().join("audit").join("sessions")
    }

    pub fn segment_path(&self, segment_index: usize) -> PathBuf {
        self.raw_dir
            .join(format!("events-{segment_index:05}.jsonl"))
    }
}

pub struct AuditWriter {
    manifest: AuditSessionManifest,
    paths: AuditPaths,
    raw_segment_max_bytes: u64,
    raw_flush_threshold_bytes: u64,
    current_segment: usize,
    current_segment_bytes: u64,
    pending_flush_bytes: u64,
    next_seq: u64,
    raw_writer: Option<BufWriter<File>>,
}

impl AuditWriter {
    pub fn create(
        root: impl AsRef<Path>,
        manifest: AuditSessionManifest,
        raw_segment_max_bytes: u64,
    ) -> Result<Self> {
        let paths = AuditPaths::new(root, &manifest.session_id);
        fs::create_dir_all(paths.raw_dir()).with_context(|| {
            format!("创建审计原始事件目录 `{}` 失败", paths.raw_dir().display())
        })?;
        fs::create_dir_all(paths.warehouse_dir()).with_context(|| {
            format!(
                "创建审计仓库目录 `{}` 失败",
                paths.warehouse_dir().display()
            )
        })?;

        write_json(paths.manifest_path(), &manifest)?;
        write_json(
            paths.summary_path(),
            &AuditSessionSummary::new_running(&manifest),
        )?;

        Ok(Self {
            manifest,
            paths,
            raw_segment_max_bytes: raw_segment_max_bytes.max(1_024),
            raw_flush_threshold_bytes: raw_segment_max_bytes
                .max(4_096)
                .min(256 * 1_024),
            current_segment: 1,
            current_segment_bytes: 0,
            pending_flush_bytes: 0,
            next_seq: 1,
            raw_writer: None,
        })
    }

    pub fn manifest(&self) -> &AuditSessionManifest {
        &self.manifest
    }

    pub fn paths(&self) -> &AuditPaths {
        &self.paths
    }

    pub fn append_event(&mut self, event: NewAuditEvent) -> Result<AuditEvent> {
        let full_event = AuditEvent {
            session_id: self.manifest.session_id.clone(),
            env: self.manifest.env.clone(),
            seq: self.next_seq,
            timestamp_ms: event.timestamp_ms,
            kind: event.kind,
            symbol: event.symbol,
            signal_id: event.signal_id,
            correlation_id: event.correlation_id,
            gate: event.gate,
            result: event.result,
            reason: event.reason,
            summary: event.summary,
            payload: event.payload,
        };
        let payload = serde_json::to_vec(&full_event).with_context(|| {
            format!(
                "序列化审计事件 `{}` 失败",
                self.paths.segment_path(self.current_segment).display()
            )
        })?;
        self.rotate_if_needed(payload.len() as u64 + 1)?;
        self.append_jsonl(&payload)?;
        self.next_seq += 1;
        Ok(full_event)
    }

    pub fn write_summary(&mut self, summary: &AuditSessionSummary) -> Result<()> {
        self.flush_raw()?;
        write_json(self.paths.summary_path(), summary)
    }

    pub fn write_checkpoint(&mut self, checkpoint: &AuditCheckpoint) -> Result<()> {
        self.flush_raw()?;
        write_json(self.paths.checkpoint_path(), checkpoint)
    }

    pub fn write_execution_context(&mut self, context: &PulseExecutionContext) -> Result<()> {
        self.flush_raw()?;
        write_json(self.paths.execution_context_path(), context)
    }

    pub fn flush_raw(&mut self) -> Result<()> {
        if let Some(writer) = self.raw_writer.as_mut() {
            writer.flush().with_context(|| {
                format!(
                    "刷新审计事件文件 `{}` 失败",
                    self.paths.segment_path(self.current_segment).display()
                )
            })?;
        }
        self.pending_flush_bytes = 0;
        Ok(())
    }

    fn rotate_if_needed(&mut self, next_entry_len: u64) -> Result<()> {
        if self.current_segment_bytes > 0
            && self.current_segment_bytes.saturating_add(next_entry_len)
                > self.raw_segment_max_bytes
        {
            self.flush_raw()?;
            self.current_segment += 1;
            self.current_segment_bytes = 0;
            self.raw_writer = None;
        }
        Ok(())
    }

    fn append_jsonl(&mut self, payload: &[u8]) -> Result<()> {
        let path = self.paths.segment_path(self.current_segment);
        if self.raw_writer.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("打开审计事件文件 `{}` 失败", path.display()))?;
            self.raw_writer = Some(BufWriter::new(file));
        }

        let writer = self
            .raw_writer
            .as_mut()
            .expect("raw writer should be initialized");
        writer
            .write_all(payload)
            .with_context(|| format!("写入审计事件 `{}` 失败", path.display()))?;
        writer
            .write_all(b"\n")
            .with_context(|| format!("写入审计换行 `{}` 失败", path.display()))?;
        self.current_segment_bytes = self
            .current_segment_bytes
            .saturating_add(payload.len() as u64 + 1);
        self.pending_flush_bytes = self
            .pending_flush_bytes
            .saturating_add(payload.len() as u64 + 1);
        if self.pending_flush_bytes >= self.raw_flush_threshold_bytes {
            writer
                .flush()
                .with_context(|| format!("刷新审计事件文件 `{}` 失败", path.display()))?;
            self.pending_flush_bytes = 0;
        }
        Ok(())
    }
}

impl Drop for AuditWriter {
    fn drop(&mut self) {
        let _ = self.flush_raw();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{model::PulseExecutionRouteContext, reader::AuditReader};
    use polyalpha_core::TradingMode;

    fn unique_test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "polyalpha-audit-writer-{label}-{}",
            std::process::id()
        ))
    }

    fn test_manifest(session_id: &str) -> AuditSessionManifest {
        AuditSessionManifest {
            version: 1,
            session_id: session_id.to_owned(),
            env: "test".to_owned(),
            mode: TradingMode::Paper,
            started_at_ms: 1_000,
            market_count: 1,
            markets: vec!["test-market".to_owned()],
        }
    }

    #[test]
    fn append_event_becomes_readable_after_explicit_flush() {
        let root = unique_test_root("readable");
        let session_id = "session-readable";
        let mut writer = AuditWriter::create(&root, test_manifest(session_id), 1_024 * 1_024)
            .expect("create writer");

        writer
            .append_event(NewAuditEvent {
                timestamp_ms: 1_001,
                kind: crate::model::AuditEventKind::Anomaly,
                symbol: Some("test-market".to_owned()),
                signal_id: None,
                correlation_id: None,
                gate: None,
                result: None,
                reason: Some("test_reason".to_owned()),
                summary: "test anomaly".to_owned(),
                payload: crate::model::AuditEventPayload::Anomaly(
                    crate::model::AuditAnomalyEvent {
                        severity: crate::model::AuditSeverity::Warning,
                        code: "test_reason".to_owned(),
                        message: "test anomaly".to_owned(),
                        symbol: Some("test-market".to_owned()),
                        signal_id: None,
                        correlation_id: None,
                    },
                ),
            })
            .expect("append event");

        let events = AuditReader::load_events(&root, session_id).expect("load events before flush");
        assert!(events.is_empty());

        writer.flush_raw().expect("flush raw");

        let events = AuditReader::load_events(&root, session_id).expect("load events after flush");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].summary, "test anomaly");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn writer_persists_execution_context_sidecar() {
        let root = unique_test_root("execution-context");
        let session_id = "session-execution-context";
        let mut writer = AuditWriter::create(&root, test_manifest(session_id), 1_024 * 1_024)
            .expect("create writer");
        let context = PulseExecutionContext {
            session_id: session_id.to_owned(),
            env: "test".to_owned(),
            mode: "paper".to_owned(),
            enabled_assets: vec!["btc".to_owned()],
            market_symbols: vec!["btc-above-100k".to_owned()],
            routes: vec![PulseExecutionRouteContext {
                asset: "btc".to_owned(),
                anchor_provider: "deribit_primary".to_owned(),
                hedge_venue: "binance_usdm_perp".to_owned(),
            }],
            poly_fee_bps: 100,
            cex_taker_fee_bps: 5,
            cex_slippage_bps: 2,
            maker_proxy_version: "book_proxy_v1".to_owned(),
            max_holding_secs: 900,
            opening_request_notional_usd: "250".to_owned(),
            min_opening_notional_usd: "50".to_owned(),
            min_expected_net_pnl_usd: "0.5".to_owned(),
            require_nonzero_hedge: true,
            min_open_fill_ratio: Some("0.05".to_owned()),
        };

        writer
            .write_execution_context(&context)
            .expect("write execution context");

        let raw = fs::read_to_string(writer.paths().execution_context_path())
            .expect("read execution context");
        let decoded: PulseExecutionContext =
            serde_json::from_str(&raw).expect("decode execution context");
        assert_eq!(decoded.session_id, session_id);
        assert_eq!(decoded.hedge_venue_for_asset("btc"), Some("binance_usdm_perp"));

        let _ = fs::remove_dir_all(root);
    }
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let payload = serde_json::to_vec_pretty(value)
        .with_context(|| format!("序列化审计文件 `{}` 失败", path.display()))?;
    fs::write(path, payload).with_context(|| format!("写入审计文件 `{}` 失败", path.display()))
}
