use std::cmp::Reverse;
use std::fs;
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
        let paths = AuditPaths::new(root, session_id);
        if !paths.raw_dir().exists() {
            return Ok(Vec::new());
        }

        let mut entries = fs::read_dir(paths.raw_dir())
            .with_context(|| format!("读取审计原始目录 `{}` 失败", paths.raw_dir().display()))?
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("events-"))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());

        let mut out = Vec::new();
        for entry in entries {
            let raw = fs::read_to_string(entry.path())
                .with_context(|| format!("读取审计事件文件 `{}` 失败", entry.path().display()))?;
            for (idx, line) in raw.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let event = serde_json::from_str(trimmed).with_context(|| {
                    format!(
                        "解析审计事件 `{}` 第 {} 行失败",
                        entry.path().display(),
                        idx + 1
                    )
                })?;
                out.push(event);
            }
        }
        Ok(out)
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
