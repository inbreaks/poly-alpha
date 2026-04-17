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
        let mut conn = self.connect()?;
        self.ensure_schema(&conn)?;
        let tx = conn.transaction().context("开启审计仓库同步事务失败")?;

        let mut existing_seq_stmt = tx
            .prepare("SELECT seq FROM audit_events WHERE session_id = ? ORDER BY seq DESC LIMIT 1")
            .with_context(|| format!("准备审计会话 `{session_id}` 已同步序号查询失败"))?;
        let mut existing_seq_rows = existing_seq_stmt
            .query_map(params![session_id], |row| row.get::<_, i64>(0))
            .with_context(|| format!("查询审计会话 `{session_id}` 已同步序号失败"))?;
        let existing_max_seq = match existing_seq_rows.next() {
            Some(row) => {
                row.with_context(|| format!("读取审计会话 `{session_id}` 已同步序号失败"))?
            }
            None => 0,
        };
        drop(existing_seq_rows);
        drop(existing_seq_stmt);

        let mut appender = tx
            .appender("audit_events")
            .context("创建审计事件批量写入器失败")?;

        AuditReader::visit_events_after_seq(root, session_id, existing_max_seq as u64, |event| {
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
            Ok(())
        })?;
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
                    entry_price,
                    actual_fill_notional_usd,
                    candidate_expected_net_pnl_usd,
                    expected_open_net_pnl_usd,
                    timeout_loss_estimate_usd,
                    required_hit_rate,
                    pulse_score_bps,
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
                    timeout_exit_price,
                    entry_executable_notional_usd,
                    reversion_pocket_ticks,
                    reversion_pocket_notional_usd,
                    vacuum_ratio,
                    anchor_latency_delta_ms,
                    distance_to_mid_bps,
                    relative_order_age_ms,
                    row_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                    pulse_summary.entry_price.clone(),
                    pulse_summary.actual_fill_notional_usd.clone(),
                    pulse_summary.candidate_expected_net_pnl_usd.clone(),
                    pulse_summary.expected_open_net_pnl_usd.clone(),
                    pulse_summary.timeout_loss_estimate_usd.clone(),
                    pulse_summary.required_hit_rate,
                    pulse_summary.pulse_score_bps,
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
                    pulse_summary.timeout_exit_price.clone(),
                    pulse_summary.entry_executable_notional_usd.clone(),
                    pulse_summary.reversion_pocket_ticks,
                    pulse_summary.reversion_pocket_notional_usd.clone(),
                    pulse_summary.vacuum_ratio.clone(),
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
                entry_price VARCHAR,
                actual_fill_notional_usd VARCHAR NOT NULL,
                candidate_expected_net_pnl_usd VARCHAR,
                expected_open_net_pnl_usd VARCHAR NOT NULL,
                timeout_loss_estimate_usd VARCHAR,
                required_hit_rate DOUBLE,
                pulse_score_bps DOUBLE,
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
                timeout_exit_price VARCHAR,
                entry_executable_notional_usd VARCHAR,
                reversion_pocket_ticks DOUBLE,
                reversion_pocket_notional_usd VARCHAR,
                vacuum_ratio VARCHAR,
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
            ("entry_price", "VARCHAR"),
            ("actual_fill_notional_usd", "VARCHAR"),
            ("candidate_expected_net_pnl_usd", "VARCHAR"),
            ("expected_open_net_pnl_usd", "VARCHAR"),
            ("timeout_loss_estimate_usd", "VARCHAR"),
            ("required_hit_rate", "DOUBLE"),
            ("pulse_score_bps", "DOUBLE"),
            ("effective_open", "BOOLEAN"),
            ("opening_outcome", "VARCHAR"),
            ("opening_rejection_reason", "VARCHAR"),
            ("opening_allocated_hedge_qty", "VARCHAR"),
            ("exit_path", "VARCHAR"),
            ("target_exit_price", "VARCHAR"),
            ("final_exit_price", "VARCHAR"),
            ("timeout_exit_price", "VARCHAR"),
            ("entry_executable_notional_usd", "VARCHAR"),
            ("reversion_pocket_ticks", "DOUBLE"),
            ("reversion_pocket_notional_usd", "VARCHAR"),
            ("vacuum_ratio", "VARCHAR"),
        ] {
            if columns.iter().any(|column| column == name) {
                continue;
            }
            conn.execute(
                &format!("ALTER TABLE pulse_session_summaries ADD COLUMN {name} {definition}"),
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
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use duckdb::Connection;
    use polyalpha_core::TradingMode;
    use uuid::Uuid;

    use super::AuditWarehouse;
    use crate::{
        AuditAnomalyEvent, AuditEvent, AuditEventKind, AuditEventPayload, AuditPaths,
        AuditReader, AuditSessionManifest, AuditSeverity, AuditWriter, NewAuditEvent,
        PulseSessionSummaryRow,
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
    fn warehouse_sync_appends_only_new_events_on_incremental_resync() {
        let root = temp_root("polyalpha-audit-incremental-sync");
        let session_id = "incremental-session";

        write_test_session(&root, session_id).expect("write first event");

        let warehouse = AuditWarehouse::new(&root).expect("create warehouse");
        warehouse
            .sync_session(&root, session_id)
            .expect("initial sync session");

        append_test_event(
            &root,
            session_id,
            1_717_171_717_002,
            "test anomaly 2",
            "test_reason_2",
        )
        .expect("append second event");
        warehouse
            .sync_session(&root, session_id)
            .expect("incremental sync session");

        let conn = Connection::open(warehouse.db_path()).expect("open warehouse db");
        let inserted_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM audit_events WHERE session_id = ?",
                duckdb::params![session_id],
                |row| row.get(0),
            )
            .expect("query inserted events");
        assert_eq!(inserted_events, 2);

        let latest_summary: String = conn
            .query_row(
                "SELECT summary FROM audit_events WHERE session_id = ? ORDER BY seq DESC LIMIT 1",
                duckdb::params![session_id],
                |row| row.get(0),
            )
            .expect("query latest summary");
        assert_eq!(latest_summary, "test anomaly 2");

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
        let row: (
            String,
            String,
            String,
            String,
            f64,
            f64,
            bool,
            String,
            String,
            String,
            String,
            String,
            f64,
            String,
            String,
        ) = conn
            .query_row(
                "SELECT entry_price, actual_fill_notional_usd, candidate_expected_net_pnl_usd, timeout_loss_estimate_usd,
                        required_hit_rate, pulse_score_bps, effective_open, opening_outcome, opening_allocated_hedge_qty, exit_path,
                        target_exit_price, timeout_exit_price, reversion_pocket_ticks,
                        reversion_pocket_notional_usd, vacuum_ratio
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
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                    ))
                },
            )
            .expect("query migrated pulse summary row");

        assert_eq!(row.0, "0.35");
        assert_eq!(row.1, "1225");
        assert_eq!(row.2, "4.12");
        assert_eq!(row.3, "21.68");
        assert!((row.4 - 0.768).abs() < f64::EPSILON);
        assert_eq!(row.5, 182.5);
        assert!(row.6);
        assert_eq!(row.7, "effective_open");
        assert_eq!(row.8, "0.39");
        assert_eq!(row.9, "maker_proxy_hit");
        assert_eq!(row.10, "0.38");
        assert_eq!(row.11, "0.31");
        assert_eq!(row.12, 4.0);
        assert_eq!(row.13, "28.57");
        assert_eq!(row.14, "1");

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
        writer.flush_raw()?;
        Ok(())
    }

    fn append_test_event(
        root: &Path,
        session_id: &str,
        timestamp_ms: u64,
        summary: &str,
        reason: &str,
    ) -> Result<()> {
        let existing = AuditReader::load_events(root, session_id)?;
        let next_seq = existing.last().map(|event| event.seq + 1).unwrap_or(1);
        let paths = AuditPaths::new(root, session_id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(paths.segment_path(1))
            .context("open raw event segment for append")?;
        let event = AuditEvent {
            session_id: session_id.to_owned(),
            env: "test".to_owned(),
            seq: next_seq,
            timestamp_ms,
            kind: AuditEventKind::Anomaly,
            symbol: Some("test-market".to_owned()),
            signal_id: None,
            correlation_id: None,
            gate: None,
            result: None,
            reason: Some(reason.to_owned()),
            summary: summary.to_owned(),
            payload: AuditEventPayload::Anomaly(AuditAnomalyEvent {
                severity: AuditSeverity::Warning,
                code: reason.to_owned(),
                message: summary.to_owned(),
                symbol: Some("test-market".to_owned()),
                signal_id: None,
                correlation_id: None,
            }),
        };
        writeln!(
            file,
            "{}",
            serde_json::to_string(&event).context("serialize appended audit event")?
        )
        .context("append raw event line")?;
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
            entry_price: Some("0.35".to_owned()),
            actual_fill_notional_usd: "1225".to_owned(),
            candidate_expected_net_pnl_usd: Some("4.12".to_owned()),
            expected_open_net_pnl_usd: "3.85".to_owned(),
            timeout_loss_estimate_usd: Some("21.68".to_owned()),
            required_hit_rate: Some(0.768),
            pulse_score_bps: Some(182.5),
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
            timeout_exit_price: Some("0.31".to_owned()),
            entry_executable_notional_usd: Some("250".to_owned()),
            reversion_pocket_ticks: Some(4.0),
            reversion_pocket_notional_usd: Some("28.57".to_owned()),
            vacuum_ratio: Some("1".to_owned()),
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
