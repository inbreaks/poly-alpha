use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::model::{
    AuditCheckpoint, AuditEvent, AuditSessionManifest, AuditSessionSummary, NewAuditEvent,
};

#[derive(Clone, Debug)]
pub struct AuditPaths {
    root: PathBuf,
    session_dir: PathBuf,
    raw_dir: PathBuf,
    manifest_path: PathBuf,
    summary_path: PathBuf,
    checkpoint_path: PathBuf,
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
    current_segment: usize,
    next_seq: u64,
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
            current_segment: 1,
            next_seq: 1,
        })
    }

    pub fn manifest(&self) -> &AuditSessionManifest {
        &self.manifest
    }

    pub fn paths(&self) -> &AuditPaths {
        &self.paths
    }

    pub fn append_event(&mut self, event: NewAuditEvent) -> Result<AuditEvent> {
        self.rotate_if_needed()?;
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
        append_jsonl(&self.paths.segment_path(self.current_segment), &full_event)?;
        self.next_seq += 1;
        Ok(full_event)
    }

    pub fn write_summary(&self, summary: &AuditSessionSummary) -> Result<()> {
        write_json(self.paths.summary_path(), summary)
    }

    pub fn write_checkpoint(&self, checkpoint: &AuditCheckpoint) -> Result<()> {
        write_json(self.paths.checkpoint_path(), checkpoint)
    }

    fn rotate_if_needed(&mut self) -> Result<()> {
        let current_path = self.paths.segment_path(self.current_segment);
        if let Ok(metadata) = fs::metadata(&current_path) {
            if metadata.len() >= self.raw_segment_max_bytes {
                self.current_segment += 1;
            }
        }
        Ok(())
    }
}

fn append_jsonl(path: &Path, item: &impl serde::Serialize) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("打开审计事件文件 `{}` 失败", path.display()))?;
    serde_json::to_writer(&mut file, item)
        .with_context(|| format!("写入审计事件 `{}` 失败", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("写入审计换行 `{}` 失败", path.display()))?;
    file.flush()
        .with_context(|| format!("刷新审计事件文件 `{}` 失败", path.display()))?;
    Ok(())
}

fn write_json(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let payload = serde_json::to_vec_pretty(value)
        .with_context(|| format!("序列化审计文件 `{}` 失败", path.display()))?;
    fs::write(path, payload).with_context(|| format!("写入审计文件 `{}` 失败", path.display()))
}
