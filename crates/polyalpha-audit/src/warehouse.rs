use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use duckdb::{params, Connection};

use crate::reader::AuditReader;
pub struct AuditWarehouse {
    db_path: PathBuf,
}

impl AuditWarehouse {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let warehouse_dir = root.as_ref().join("audit").join("warehouse");
        std::fs::create_dir_all(&warehouse_dir)
            .with_context(|| format!("创建审计仓库目录 `{}` 失败", warehouse_dir.display()))?;
        let db_path = warehouse_dir.join("audit.duckdb");
        Ok(Self { db_path })
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn sync_session(&self, root: impl AsRef<Path>, session_id: &str) -> Result<()> {
        let root = root.as_ref();
        let summary = AuditReader::load_summary(root, session_id)?;
        let events = AuditReader::load_events(root, session_id)?;
        let mut conn = self.connect()?;
        self.ensure_schema(&conn)?;
        let tx = conn
            .transaction()
            .context("开启审计仓库同步事务失败")?;

        let existing_max_seq = tx
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM audit_events WHERE session_id = ?",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .with_context(|| format!("查询审计会话 `{session_id}` 已同步序号失败"))?;

        let mut appender = tx
            .appender("audit_events")
            .context("创建审计事件批量写入器失败")?;

        for event in events
            .into_iter()
            .filter(|event| event.seq > existing_max_seq as u64)
        {
            let payload_json =
                serde_json::to_string(&event.payload).context("序列化审计事件负载失败")?;
            appender
                .append_row(params![
                event.session_id,
                event.env,
                event.seq as i64,
                event.timestamp_ms as i64,
                event.kind.as_str(),
                event.symbol,
                event.signal_id,
                event.correlation_id,
                event.gate,
                event.result,
                event.reason,
                event.summary,
                payload_json,
            ])
                .with_context(|| format!("写入审计事件 seq={} 失败", event.seq))?;
        }
        appender.flush().context("刷新审计事件批量写入器失败")?;
        drop(appender);

        tx.execute(
            "DELETE FROM audit_sessions WHERE session_id = ?",
            params![summary.session_id.clone()],
        )
        .with_context(|| format!("删除旧审计摘要 `{}` 失败", summary.session_id))?;
        tx.execute(
            "INSERT INTO audit_sessions (
                session_id, env, mode, status, started_at_ms, ended_at_ms, updated_at_ms,
                market_count, markets_json, latest_tick_index, latest_checkpoint_ms,
                latest_equity, latest_total_pnl_usd, latest_today_pnl_usd, latest_total_exposure_usd,
                counters_json, rejection_reasons_json, skip_reasons_json, summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                summary.session_id.clone(),
                summary.env.clone(),
                format!("{:?}", summary.mode),
                format!("{:?}", summary.status),
                summary.started_at_ms as i64,
                summary.ended_at_ms.map(|value| value as i64),
                summary.updated_at_ms as i64,
                summary.market_count as i64,
                serde_json::to_string(&summary.markets).context("序列化审计市场列表失败")?,
                summary.latest_tick_index as i64,
                summary.latest_checkpoint_ms.map(|value| value as i64),
                summary.latest_equity,
                summary.latest_total_pnl_usd,
                summary.latest_today_pnl_usd,
                summary.latest_total_exposure_usd,
                serde_json::to_string(&summary.counters).context("序列化审计计数器失败")?,
                serde_json::to_string(&summary.rejection_reasons)
                    .context("序列化信号拒绝原因失败")?,
                serde_json::to_string(&summary.skip_reasons).context("序列化评估跳过原因失败")?,
                serde_json::to_string(&summary).context("序列化审计摘要失败")?,
            ],
        )
        .with_context(|| format!("写入审计摘要 `{}` 失败", summary.session_id))?;
        tx.commit()
            .with_context(|| format!("提交审计会话 `{session_id}` 仓库同步事务失败"))?;

        Ok(())
    }

    fn connect(&self) -> Result<Connection> {
        Connection::open(&self.db_path)
            .with_context(|| format!("打开审计仓库 `{}` 失败", self.db_path.display()))
    }

    fn ensure_schema(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS audit_events (
                session_id VARCHAR NOT NULL,
                env VARCHAR NOT NULL,
                seq BIGINT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                kind VARCHAR NOT NULL,
                symbol VARCHAR,
                signal_id VARCHAR,
                correlation_id VARCHAR,
                gate VARCHAR,
                result VARCHAR,
                reason VARCHAR,
                summary VARCHAR NOT NULL,
                payload_json VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_sessions (
                session_id VARCHAR PRIMARY KEY,
                env VARCHAR NOT NULL,
                mode VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                started_at_ms BIGINT NOT NULL,
                ended_at_ms BIGINT,
                updated_at_ms BIGINT NOT NULL,
                market_count BIGINT NOT NULL,
                markets_json VARCHAR NOT NULL,
                latest_tick_index BIGINT NOT NULL,
                latest_checkpoint_ms BIGINT,
                latest_equity DOUBLE,
                latest_total_pnl_usd DOUBLE,
                latest_today_pnl_usd DOUBLE,
                latest_total_exposure_usd DOUBLE,
                counters_json VARCHAR NOT NULL,
                rejection_reasons_json VARCHAR NOT NULL,
                skip_reasons_json VARCHAR NOT NULL,
                summary_json VARCHAR NOT NULL
            );
            ",
        )
        .context("初始化审计仓库 schema 失败")
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use duckdb::Connection;
    use polyalpha_core::TradingMode;
    use uuid::Uuid;

    use super::AuditWarehouse;
    use crate::{
        AuditAnomalyEvent, AuditEventKind, AuditEventPayload, AuditSessionManifest, AuditSeverity,
        AuditWriter, NewAuditEvent,
    };

    #[test]
    fn sync_session_rolls_back_event_inserts_when_summary_write_fails() {
        let root = temp_root("polyalpha-audit-sync-session");
        let session_id = "session-atomicity";

        write_test_session(&root, session_id).expect("write test session");

        let warehouse = AuditWarehouse::new(&root).expect("create warehouse");
        create_incompatible_summary_schema(warehouse.db_path())
            .expect("create incompatible summary schema");

        let err = warehouse
            .sync_session(&root, session_id)
            .expect_err("sync should fail with incompatible audit_sessions schema");
        assert!(
            err.to_string().contains("写入审计摘要"),
            "unexpected error: {err:#}"
        );

        let conn = Connection::open(warehouse.db_path()).expect("open warehouse db");
        let inserted_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM audit_events WHERE session_id = ?",
                duckdb::params![session_id],
                |row| row.get(0),
            )
            .expect("query inserted events");
        assert_eq!(
            inserted_events, 0,
            "failed sync should not leave partially inserted events"
        );

        let _ = fs::remove_dir_all(&root);
    }

    fn temp_root(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!("{prefix}-{}", Uuid::new_v4()))
    }

    fn write_test_session(root: &Path, session_id: &str) -> Result<()> {
        let manifest = AuditSessionManifest {
            version: 1,
            session_id: session_id.to_owned(),
            env: "test".to_owned(),
            mode: TradingMode::Paper,
            started_at_ms: 1_717_171_717_000,
            market_count: 1,
            markets: vec!["test-market".to_owned()],
        };
        let mut writer = AuditWriter::create(root, manifest, 1_024 * 1_024)?;
        writer.append_event(NewAuditEvent {
            timestamp_ms: 1_717_171_717_001,
            kind: AuditEventKind::Anomaly,
            symbol: Some("test-market".to_owned()),
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: Some("test_failure".to_owned()),
            summary: "test anomaly".to_owned(),
            payload: AuditEventPayload::Anomaly(AuditAnomalyEvent {
                severity: AuditSeverity::Warning,
                code: "test_failure".to_owned(),
                message: "test anomaly".to_owned(),
                symbol: Some("test-market".to_owned()),
                signal_id: None,
                correlation_id: None,
            }),
        })?;
        Ok(())
    }

    fn create_incompatible_summary_schema(db_path: &Path) -> Result<()> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "
            CREATE TABLE audit_events (
                session_id VARCHAR NOT NULL,
                env VARCHAR NOT NULL,
                seq BIGINT NOT NULL,
                timestamp_ms BIGINT NOT NULL,
                kind VARCHAR NOT NULL,
                symbol VARCHAR,
                signal_id VARCHAR,
                correlation_id VARCHAR,
                gate VARCHAR,
                result VARCHAR,
                reason VARCHAR,
                summary VARCHAR NOT NULL,
                payload_json VARCHAR NOT NULL
            );

            CREATE TABLE audit_sessions (
                session_id VARCHAR PRIMARY KEY
            );
            ",
        )
        .context("prepare incompatible warehouse schema")
    }
}
