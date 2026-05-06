use std::cmp::Reverse;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};

use crate::model::{AuditCheckpoint, AuditEvent, AuditSessionSummary};
use crate::writer::AuditPaths;

pub struct AuditReader;

impl AuditReader {
    pub fn load_summary(root: impl AsRef<Path>, session_id: &str) -> Result<AuditSessionSummary> {
        let paths = AuditPaths::new(root, session_id);
        let raw = fs::read_to_string(paths.summary_path())
            .with_context(|| format!("读取审计摘要 `{}` 失败", paths.summary_path().display()))?;
        serde_json::from_str(&raw)
            .with_context(|| format!("解析审计摘要 `{}` 失败", paths.summary_path().display()))
    }

    pub fn load_checkpoint(
        root: impl AsRef<Path>,
        session_id: &str,
    ) -> Result<Option<AuditCheckpoint>> {
        let paths = AuditPaths::new(root, session_id);
        if !paths.checkpoint_path().exists() {
            return Ok(None);
        }
        let raw = fs::read_to_string(paths.checkpoint_path()).with_context(|| {
            format!(
                "读取审计检查点 `{}` 失败",
                paths.checkpoint_path().display()
            )
        })?;
        let checkpoint = serde_json::from_str(&raw).with_context(|| {
            format!(
                "解析审计检查点 `{}` 失败",
                paths.checkpoint_path().display()
            )
        })?;
        Ok(Some(checkpoint))
    }

    pub fn load_events(root: impl AsRef<Path>, session_id: &str) -> Result<Vec<AuditEvent>> {
        let mut out = Vec::new();
        Self::visit_events_after_seq(root, session_id, 0, |event| {
            out.push(event.clone());
            Ok(())
        })?;
        Ok(out)
    }

    pub fn visit_events_after_seq<F>(
        root: impl AsRef<Path>,
        session_id: &str,
        after_seq: u64,
        mut visitor: F,
    ) -> Result<()>
    where
        F: FnMut(&AuditEvent) -> Result<()>,
    {
        let paths = AuditPaths::new(root, session_id);
        for entry in raw_event_entries(&paths)? {
            let file = fs::File::open(entry.path())
                .with_context(|| format!("打开审计事件文件 `{}` 失败", entry.path().display()))?;
            let reader = BufReader::new(file);
            for (idx, line) in reader.lines().enumerate() {
                let line = line.with_context(|| {
                    format!(
                        "读取审计事件 `{}` 第 {} 行失败",
                        entry.path().display(),
                        idx + 1
                    )
                })?;
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let event: AuditEvent = serde_json::from_str(trimmed).with_context(|| {
                    format!(
                        "解析审计事件 `{}` 第 {} 行失败",
                        entry.path().display(),
                        idx + 1
                    )
                })?;
                if event.seq <= after_seq {
                    continue;
                }
                visitor(&event)?;
            }
        }
        Ok(())
    }

    pub fn latest_session_summary(
        root: impl AsRef<Path>,
        env: Option<&str>,
    ) -> Result<Option<AuditSessionSummary>> {
        let mut summaries = Self::list_session_summaries(root, env, 1)?;
        Ok(summaries.pop())
    }

    pub fn list_session_summaries(
        root: impl AsRef<Path>,
        env: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AuditSessionSummary>> {
        let sessions_root = AuditPaths::sessions_root(root);
        if !sessions_root.exists() {
            return Ok(Vec::new());
        }

        let mut summaries = Vec::new();
        for entry in fs::read_dir(&sessions_root)
            .with_context(|| format!("读取审计会话目录 `{}` 失败", sessions_root.display()))?
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            let summary_path = entry.path().join("summary.json");
            if !summary_path.exists() {
                continue;
            }
            let raw = match fs::read_to_string(&summary_path) {
                Ok(raw) => raw,
                Err(_) => continue,
            };
            let summary: AuditSessionSummary = match serde_json::from_str(&raw) {
                Ok(summary) => summary,
                Err(_) => continue,
            };
            if env.is_some_and(|expected| summary.env != expected) {
                continue;
            }
            summaries.push(summary);
        }

        summaries.sort_by_key(|summary| Reverse(summary.updated_at_ms));
        summaries.truncate(limit);
        Ok(summaries)
    }
}

fn raw_event_entries(paths: &AuditPaths) -> Result<Vec<fs::DirEntry>> {
    if !paths.raw_dir().exists() {
        return Ok(Vec::new());
    }

    let mut entries = fs::read_dir(paths.raw_dir())
        .with_context(|| format!("读取审计原始目录 `{}` 失败", paths.raw_dir().display()))?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("events-"))
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use polyalpha_core::TradingMode;
    use uuid::Uuid;

    use super::AuditReader;
    use crate::{
        AuditAnomalyEvent, AuditEventKind, AuditEventPayload, AuditSessionManifest, AuditSeverity,
        AuditWriter, NewAuditEvent,
    };

    #[test]
    fn visit_events_after_seq_streams_only_new_rows_in_order() {
        let root = temp_root("polyalpha-audit-reader");
        let session_id = "reader-session";
        let manifest = AuditSessionManifest {
            version: 1,
            session_id: session_id.to_owned(),
            env: "test".to_owned(),
            mode: TradingMode::Paper,
            started_at_ms: 1_000,
            market_count: 1,
            markets: vec!["test-market".to_owned()],
        };
        let mut writer =
            AuditWriter::create(&root, manifest, 1_024 * 1_024).expect("create writer");
        for idx in 1..=3_u64 {
            writer
                .append_event(NewAuditEvent {
                    timestamp_ms: 1_000 + idx,
                    kind: AuditEventKind::Anomaly,
                    symbol: Some("test-market".to_owned()),
                    signal_id: None,
                    correlation_id: None,
                    gate: None,
                    result: None,
                    reason: Some(format!("reason-{idx}")),
                    summary: format!("event-{idx}"),
                    payload: AuditEventPayload::Anomaly(AuditAnomalyEvent {
                        severity: AuditSeverity::Warning,
                        code: format!("code-{idx}"),
                        message: format!("event-{idx}"),
                        symbol: Some("test-market".to_owned()),
                        signal_id: None,
                        correlation_id: None,
                    }),
                })
                .expect("append event");
        }
        writer.flush_raw().expect("flush raw");

        let mut summaries = Vec::new();
        AuditReader::visit_events_after_seq(&root, session_id, 1, |event| {
            summaries.push((event.seq, event.summary.clone()));
            Ok(())
        })
        .expect("visit events after seq");

        assert_eq!(
            summaries,
            vec![(2, "event-2".to_owned()), (3, "event-3".to_owned()),]
        );

        let _ = fs::remove_dir_all(root);
    }

    fn temp_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
    }
}
