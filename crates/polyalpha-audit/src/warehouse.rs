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
        let conn = self.connect()?;
        self.ensure_schema(&conn)?;

        let existing_max_seq = conn
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM audit_events WHERE session_id = ?",
                params![session_id],
                |row| row.get::<_, i64>(0),
            )
            .with_context(|| format!("查询审计会话 `{session_id}` 已同步序号失败"))?;

        let mut stmt = conn
            .prepare(
                "INSERT INTO audit_events (
                    session_id, env, seq, timestamp_ms, kind, symbol, signal_id, correlation_id,
                    gate, result, reason, summary, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .context("准备审计事件写入语句失败")?;

        for event in events
            .into_iter()
            .filter(|event| event.seq > existing_max_seq as u64)
        {
            let payload_json =
                serde_json::to_string(&event.payload).context("序列化审计事件负载失败")?;
            stmt.execute(params![
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

        conn.execute(
            "DELETE FROM audit_sessions WHERE session_id = ?",
            params![summary.session_id.clone()],
        )
        .with_context(|| format!("删除旧审计摘要 `{}` 失败", summary.session_id))?;
        conn.execute(
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
