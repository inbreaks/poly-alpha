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
        let tx = conn.transaction().context("开启审计仓库同步事务失败")?;

        let mut existing_seq_stmt = tx
            .prepare(
                "SELECT seq FROM audit_events WHERE session_id = ? ORDER BY seq DESC LIMIT 1",
            )
            .with_context(|| format!("准备审计会话 `{session_id}` 已同步序号查询失败"))?;
        let mut existing_seq_rows = existing_seq_stmt
            .query_map(params![session_id], |row| row.get::<_, i64>(0))
            .with_context(|| format!("查询审计会话 `{session_id}` 已同步序号失败"))?;
        let existing_max_seq = match existing_seq_rows.next() {
            Some(row) => row.with_context(|| format!("读取审计会话 `{session_id}` 已同步序号失败"))?,
            None => 0,
        };
        drop(existing_seq_rows);
        drop(existing_seq_stmt);

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

        tx.execute(
            "DELETE FROM pulse_session_summaries WHERE session_id = ?",
            params![summary.session_id.clone()],
        )
        .with_context(|| format!("删除旧 pulse 会话摘要 `{}` 失败", summary.session_id))?;

        for pulse_summary in &summary.pulse_session_summaries {
            tx.execute(
                "INSERT INTO pulse_session_summaries (
                    session_id,
                    pulse_session_id,
                    asset,
                    state,
                    opened_at_ms,
                    closed_at_ms,
                    planned_poly_qty,
                    actual_poly_filled_qty,
                    actual_poly_fill_ratio,
                    actual_fill_notional_usd,
                    expected_open_net_pnl_usd,
                    effective_open,
                    opening_outcome,
                    opening_rejection_reason,
                    opening_allocated_hedge_qty,
                    session_target_delta_exposure,
                    session_allocated_hedge_qty,
                    net_edge_bps,
                    realized_pnl_usd,
                    exit_path,
                    target_exit_price,
                    final_exit_price,
                    anchor_latency_delta_ms,
                    distance_to_mid_bps,
                    relative_order_age_ms,
                    row_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    summary.session_id.clone(),
                    pulse_summary.pulse_session_id.clone(),
                    pulse_summary.asset.clone(),
                    pulse_summary.state.clone(),
                    pulse_summary.opened_at_ms as i64,
                    pulse_summary.closed_at_ms.map(|value| value as i64),
                    pulse_summary.planned_poly_qty.clone(),
                    pulse_summary.actual_poly_filled_qty.clone(),
                    pulse_summary.actual_poly_fill_ratio,
                    pulse_summary.actual_fill_notional_usd.clone(),
                    pulse_summary.expected_open_net_pnl_usd.clone(),
                    pulse_summary.effective_open,
                    pulse_summary.opening_outcome.clone(),
                    pulse_summary.opening_rejection_reason.clone(),
                    pulse_summary.opening_allocated_hedge_qty.clone(),
                    pulse_summary.session_target_delta_exposure.clone(),
                    pulse_summary.session_allocated_hedge_qty.clone(),
                    pulse_summary.net_edge_bps,
                    pulse_summary.realized_pnl_usd,
                    pulse_summary.exit_path.clone(),
                    pulse_summary.target_exit_price.clone(),
                    pulse_summary.final_exit_price.clone(),
                    pulse_summary.anchor_latency_delta_ms.map(|value| value as i64),
                    pulse_summary.distance_to_mid_bps,
                    pulse_summary.relative_order_age_ms.map(|value| value as i64),
                    serde_json::to_string(pulse_summary)
                        .context("序列化 pulse 会话摘要失败")?,
                ])
                .with_context(|| {
                    format!(
                        "写入 pulse 会话摘要 `{}` / `{}` 失败",
                        summary.session_id, pulse_summary.pulse_session_id
                    )
                })?;
        }
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

            CREATE TABLE IF NOT EXISTS pulse_session_summaries (
                session_id VARCHAR NOT NULL,
                pulse_session_id VARCHAR NOT NULL,
                asset VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                opened_at_ms BIGINT NOT NULL,
                closed_at_ms BIGINT,
                planned_poly_qty VARCHAR NOT NULL,
                actual_poly_filled_qty VARCHAR NOT NULL,
                actual_poly_fill_ratio DOUBLE NOT NULL,
                actual_fill_notional_usd VARCHAR NOT NULL,
                expected_open_net_pnl_usd VARCHAR NOT NULL,
                effective_open BOOLEAN NOT NULL,
                opening_outcome VARCHAR NOT NULL,
                opening_rejection_reason VARCHAR,
                opening_allocated_hedge_qty VARCHAR NOT NULL,
                session_target_delta_exposure VARCHAR NOT NULL,
                session_allocated_hedge_qty VARCHAR NOT NULL,
                net_edge_bps DOUBLE,
                realized_pnl_usd DOUBLE,
                exit_path VARCHAR,
                target_exit_price VARCHAR,
                final_exit_price VARCHAR,
                anchor_latency_delta_ms BIGINT,
                distance_to_mid_bps DOUBLE,
                relative_order_age_ms BIGINT,
                row_json VARCHAR NOT NULL
            );
            ",
        )
        .context("初始化审计仓库 schema 失败")?;

        self.ensure_pulse_session_summary_columns(conn)
    }

    fn ensure_pulse_session_summary_columns(&self, conn: &Connection) -> Result<()> {
        let mut stmt = conn
            .prepare("PRAGMA table_info('pulse_session_summaries')")
            .context("查询 pulse 会话摘要 schema 失败")?;
        let columns = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .context("读取 pulse 会话摘要 schema 行失败")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("收集 pulse 会话摘要 schema 列失败")?;

        for (name, definition) in [
            ("actual_fill_notional_usd", "VARCHAR"),
            ("expected_open_net_pnl_usd", "VARCHAR"),
            ("effective_open", "BOOLEAN"),
            ("opening_outcome", "VARCHAR"),
            ("opening_rejection_reason", "VARCHAR"),
            ("opening_allocated_hedge_qty", "VARCHAR"),
            ("exit_path", "VARCHAR"),
            ("target_exit_price", "VARCHAR"),
            ("final_exit_price", "VARCHAR"),
        ] {
            if columns.iter().any(|column| column == name) {
                continue;
            }
            conn.execute(
                &format!(
                    "ALTER TABLE pulse_session_summaries ADD COLUMN {name} {definition}"
                ),
                [],
            )
            .with_context(|| format!("为 pulse 会话摘要补列 `{name}` 失败"))?;
        }

        Ok(())
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
        AuditAnomalyEvent, AuditEventKind, AuditEventPayload, AuditReader, AuditSessionManifest,
        AuditSeverity, AuditWriter, NewAuditEvent, PulseSessionSummaryRow,
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

    #[test]
    fn warehouse_sync_persists_pulse_session_summary_rows() {
        let root = temp_root("polyalpha-audit-pulse-session");
        let session_id = "pulse-audit-session";

        write_test_pulse_session(&root, session_id).expect("write pulse session");

        let warehouse = AuditWarehouse::new(&root).expect("create warehouse");
        warehouse
            .sync_session(&root, session_id)
            .expect("sync pulse session");

        let conn = duckdb::Connection::open(warehouse.db_path()).expect("open warehouse");
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pulse_session_summaries WHERE session_id = ?",
                duckdb::params![session_id],
                |row| row.get(0),
            )
            .expect("query pulse summary count");

        assert_eq!(count, 1);
        let outcome: String = conn
            .query_row(
                "SELECT json_extract_string(row_json, '$.opening_outcome') FROM pulse_session_summaries WHERE session_id = ?",
                duckdb::params![session_id],
                |row| row.get(0),
            )
            .expect("query opening outcome");
        assert_eq!(outcome, "effective_open");

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn warehouse_sync_migrates_legacy_pulse_session_summary_schema() {
        let root = temp_root("polyalpha-audit-pulse-legacy-schema");
        let session_id = "pulse-audit-legacy-session";

        write_test_pulse_session(&root, session_id).expect("write pulse session");

        let warehouse = AuditWarehouse::new(&root).expect("create warehouse");
        create_legacy_pulse_summary_schema(warehouse.db_path())
            .expect("create legacy pulse summary schema");

        warehouse
            .sync_session(&root, session_id)
            .expect("sync pulse session into legacy warehouse");

        let conn = duckdb::Connection::open(warehouse.db_path()).expect("open warehouse");
        let row: (String, bool, String, String, String, String, String) = conn
            .query_row(
                "SELECT actual_fill_notional_usd, effective_open, opening_outcome, opening_allocated_hedge_qty,
                        exit_path, target_exit_price, final_exit_price
                 FROM pulse_session_summaries
                 WHERE session_id = ?",
                duckdb::params![session_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .expect("query migrated pulse summary row");

        assert_eq!(row.0, "1225");
        assert!(row.1);
        assert_eq!(row.2, "effective_open");
        assert_eq!(row.3, "0.39");
        assert_eq!(row.4, "maker_proxy_hit");
        assert_eq!(row.5, "0.38");
        assert_eq!(row.6, "0.38");

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

    fn write_test_pulse_session(root: &Path, session_id: &str) -> Result<()> {
        write_test_session(root, session_id)?;

        let mut summary = AuditReader::load_summary(root, session_id)?;
        summary.pulse_session_summaries = vec![PulseSessionSummaryRow {
            pulse_session_id: "pulse-session-1".to_owned(),
            asset: "btc".to_owned(),
            state: "maker_exit_working".to_owned(),
            opened_at_ms: 1_717_171_717_010,
            closed_at_ms: Some(1_717_171_717_999),
            planned_poly_qty: "10000".to_owned(),
            actual_poly_filled_qty: "3500".to_owned(),
            actual_poly_fill_ratio: 0.35,
            actual_fill_notional_usd: "1225".to_owned(),
            expected_open_net_pnl_usd: "3.85".to_owned(),
            effective_open: true,
            opening_outcome: "effective_open".to_owned(),
            opening_rejection_reason: None,
            opening_allocated_hedge_qty: "0.39".to_owned(),
            session_target_delta_exposure: "0.41".to_owned(),
            session_allocated_hedge_qty: "0.39".to_owned(),
            net_edge_bps: Some(31.4),
            realized_pnl_usd: Some(75.7),
            exit_path: Some("maker_proxy_hit".to_owned()),
            target_exit_price: Some("0.38".to_owned()),
            final_exit_price: Some("0.38".to_owned()),
            anchor_latency_delta_ms: Some(42),
            distance_to_mid_bps: Some(8.0),
            relative_order_age_ms: Some(950),
        }];

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
        writer.write_summary(&summary)?;
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

    fn create_legacy_pulse_summary_schema(db_path: &Path) -> Result<()> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "
            CREATE TABLE pulse_session_summaries (
                session_id VARCHAR NOT NULL,
                pulse_session_id VARCHAR NOT NULL,
                asset VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                opened_at_ms BIGINT NOT NULL,
                closed_at_ms BIGINT,
                planned_poly_qty VARCHAR NOT NULL,
                actual_poly_filled_qty VARCHAR NOT NULL,
                actual_poly_fill_ratio DOUBLE NOT NULL,
                session_target_delta_exposure VARCHAR NOT NULL,
                session_allocated_hedge_qty VARCHAR NOT NULL,
                net_edge_bps DOUBLE,
                realized_pnl_usd DOUBLE,
                anchor_latency_delta_ms BIGINT,
                distance_to_mid_bps DOUBLE,
                relative_order_age_ms BIGINT,
                row_json VARCHAR NOT NULL
            );
            ",
        )
        .context("prepare legacy pulse summary schema")
    }
}
