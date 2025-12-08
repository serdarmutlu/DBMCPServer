# tools/postgresql_observability_tools.py

from fastmcp import FastMCP, Context
from fastmcp.tools.tool import ToolResult

from db.postgresql.postgresql_manager import postgresql_manager

from utils.generic import text_result as text_result


def register_postgresql_observability_tools(mcp: FastMCP) -> None:
    """
    PostgreSQL Observability MCP Pack içindeki tüm tool'ları kaydeder.
    """

    # 1️⃣ GENEL SAĞLIK ÖZETİ
    @mcp.tool(
        name="pg_health_overview",
        description="PostgreSQL için genel sağlık özeti (connection sayıları, cache hit ratio, temp kullanımı vb.)."
    )
    async def pg_health_overview(
        ctx: Context,
        connection_id: int,
    ) -> ToolResult:
        sql = """
        SELECT
            now()                                     AS collected_at,
            current_database()                        AS db_name,
            numbackends,
            xact_commit,
            xact_rollback,
            blks_read,
            blks_hit,
            CASE
                WHEN (blks_hit + blks_read) = 0 THEN NULL
                ELSE round(blks_hit::numeric * 100.0 / (blks_hit + blks_read), 2)
            END                                       AS cache_hit_ratio,
            tup_returned,
            tup_fetched,
            tup_inserted,
            tup_updated,
            tup_deleted,
            deadlocks,
            temp_files,
            temp_bytes,
            blk_read_time,
            blk_write_time
        FROM pg_stat_database
        WHERE datname = current_database();
        """
        rows = await postgresql_manager.execute_query(connection_id, sql)
        return text_result(rows, title="PostgreSQL Health Overview")

    # 2️⃣ CONNECTION RAPORU
    @mcp.tool(
        name="pg_connections_report",
        description="Aktif/idle/idle in transaction bağlantı sayıları, en uzun transaction süreleri vb."
    )

    async def pg_connections_report(
        ctx: Context,
        connection_id: int,
    ) -> ToolResult:
        sql = """
        SELECT
            count(*)                                            AS total_connections,
            count(*) FILTER (WHERE state = 'active')            AS active_connections,
            count(*) FILTER (WHERE state = 'idle')              AS idle_connections,
            count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_tx_connections,
            max(EXTRACT(EPOCH FROM (now() - xact_start)))       AS max_tx_age_seconds,
            max(EXTRACT(EPOCH FROM (now() - query_start)))      AS max_query_age_seconds
        FROM pg_stat_activity
        WHERE datname = current_database();
        """
        details_sql = """
        SELECT
            pid,
            usename,
            application_name,
            client_addr,
            state,
            EXTRACT(EPOCH FROM (now() - xact_start))  AS tx_age_seconds,
            EXTRACT(EPOCH FROM (now() - query_start)) AS query_age_seconds,
            wait_event_type,
            wait_event,
            substring(query, 1, 200)                  AS query_sample
        FROM pg_stat_activity
        WHERE datname = current_database()
        ORDER BY tx_age_seconds DESC NULLS LAST
        LIMIT 20;
        """
        summary = await postgresql_manager.execute_query(connection_id, sql)
        details = await postgresql_manager.execute_query(connection_id, details_sql)
        payload = {
            "summary": summary[0] if summary else {},
            "top_long_running": details,
        }
        return text_result(payload, title="PostgreSQL Connections Report")

    # 3️⃣ TOP QUERIES (pg_stat_statements)
    @mcp.tool(
        name="pg_top_queries_report",
        description="pg_stat_statements üzerinden en pahalı sorguları listeler (top N, total_time'e göre)."
    )
    async def pg_top_queries_report(
        ctx: Context,
        connection_id: int,
        limit: int = 10,
    ) -> ToolResult:
        sql = """
        SELECT
            queryid,
            calls,
            round(total_exec_time::numeric, 2) AS total_exec_time_ms,
            round(mean_exec_time::numeric, 2)  AS mean_exec_time_ms,
            rows,
            shared_blks_hit,
            shared_blks_read,
            CASE
                WHEN (shared_blks_hit + shared_blks_read) = 0 THEN NULL
                ELSE round(shared_blks_hit::numeric * 100.0 /
                           (shared_blks_hit + shared_blks_read), 2)
            END                                            AS cache_hit_ratio,
            substring(query, 1, 500)                      AS query_sample
        FROM pg_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
        ORDER BY total_exec_time DESC
        LIMIT $1;
        """
        rows = await postgresql_manager.execute_query(
            connection_id,
            sql,
            limit
        )
        return text_result(rows, title=f"Top {limit} Queries (pg_stat_statements)")

    # 4️⃣ BLOAT RAPORU (pgstattuple varsa + fallback)
    @mcp.tool(
        name="pg_bloat_report",
        description="Table bloat analizi. pgstattuple varsa onu, yoksa pg_stat_user_tables dead tuple oranını kullanır."
    )
    async def pg_bloat_report(
        ctx: Context,
        connection_id: int,
        limit: int = 20,
    ) -> ToolResult:
        # Önce pgstattuple kurulu mu kontrol et
        has_ext_sql = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_available_extensions
            WHERE name = 'pgstattuple'
              AND installed_version IS NOT NULL
        ) AS has_pgstattuple;
        """
        ext_rows = await postgresql_manager.execute_query(connection_id, has_ext_sql)
        has_pgstattuple = bool(ext_rows and ext_rows[0].get("has_pgstattuple"))

        # Dead tuple oranına göre aday tabloları al
        candidate_sql = """
        SELECT
            schemaname,
            relname,
            n_live_tup,
            n_dead_tup,
            CASE
                WHEN (n_live_tup + n_dead_tup) = 0 THEN 0
                ELSE round(n_dead_tup::numeric * 100.0 /
                           (n_live_tup + n_dead_tup), 2)
            END AS dead_tuple_pct
        FROM pg_stat_user_tables
        ORDER BY dead_tuple_pct DESC
        LIMIT $1;
        """
        candidates = await postgresql_manager.execute_query(connection_id, candidate_sql, limit)

        if not has_pgstattuple:
            # Sadece dead_tuple_pct bazlı rapor
            payload = {
                "mode": "dead_tuple_only_fallback",
                "tables": candidates,
                "note": "pgstattuple yüklü değil; sadece pg_stat_user_tables bazlı tahmini bloat bilgisi gösteriliyor."
            }
            return text_result(payload, title="Table Bloat Report (Fallback Mode)")

        # pgstattuple varsa, her tablo için detaylı bloat ölç
        detailed = []
        for row in candidates:
            schemaname = row["schemaname"]
            relname = row["relname"]
            tbl_expr = f"{schemaname}.{relname}"
            bloat_sql = f"SELECT * FROM pgstattuple('{tbl_expr}');"
            try:
                bloat_rows = await postgresql_manager.execute_query(connection_id, bloat_sql)
                if bloat_rows:
                    entry = {
                        "schemaname": schemaname,
                        "relname": relname,
                        "dead_tuple_pct_estimated": row["dead_tuple_pct"],
                        "pgstattuple": bloat_rows[0],
                    }
                    detailed.append(entry)
            except Exception as ex:
                detailed.append({
                    "schemaname": schemaname,
                    "relname": relname,
                    "dead_tuple_pct_estimated": row["dead_tuple_pct"],
                    "error": str(ex),
                })

        payload = {
            "mode": "pgstattuple",
            "tables": detailed,
        }
        return text_result(payload, title="Table Bloat Report (pgstattuple)")

    # 5️⃣ AUTOVACUUM ACTIVITY
    @mcp.tool(
        name="pg_autovacuum_activity",
        description="Şu anda çalışan VACUUM süreçlerini ve tabloların son vacuum/analyze zamanlarını listeler."
    )
    async def pg_autovacuum_activity(
        ctx: Context,
        connection_id: int,
    ) -> ToolResult:
        progress_sql = """
                       SELECT datname, \
                              relid::regclass AS table_name, phase, \
                              heap_blks_total, \
                              heap_blks_scanned, \
                              heap_blks_vacuumed, \
                              index_vacuum_count
                       FROM pg_stat_progress_vacuum
                       WHERE datname = current_database(); \
                       """

        stats_sql = """
                    SELECT schemaname, \
                           relname, \
                           n_live_tup, \
                           n_dead_tup, \
                           last_vacuum, \
                           last_autovacuum, \
                           last_analyze, \
                           last_autoanalyze
                    FROM pg_stat_user_tables
                    ORDER BY n_dead_tup DESC LIMIT 50; \
                    """
        progress = await postgresql_manager.execute_query(connection_id, progress_sql)
        stats = await postgresql_manager.execute_query(connection_id, stats_sql)

        return text_result(
            {
                "active_vacuum_processes": progress,
                "table_health": stats
            },
            title="Autovacuum Activity & Table Health"
        )

    # 6️⃣ WAL / CHECKPOINT RAPORU
    @mcp.tool(
        name="pg_wal_activity_report",
        description="pg_stat_bgwriter üzerinden checkpoint, buffer ve WAL davranışını raporlar."
    )
    async def pg_wal_activity_report(
        ctx: Context,
        connection_id: int,
    ) -> ToolResult:
        ver_sql = """
        SELECT current_setting('server_version_num')::int AS version_num;
        """
        ver = await postgresql_manager.execute_query(connection_id, ver_sql)
        server_version = ver[0]["version_num"]

        payload = {}

        # --- BGWRITER (tüm sürümler)
        payload["bgwriter"] = await postgresql_manager.execute_query(
            connection_id,
            """
            SELECT buffers_clean,
                   maxwritten_clean,
                   stats_reset
            FROM pg_stat_bgwriter;
            """
        )

        # --- CHECKPOINTER
        if server_version >= 150000:
            payload["checkpointer"] = await postgresql_manager.execute_query(
                connection_id,
                """
                SELECT num_timed,
                       num_requested,
                       buffers_written,
                       write_time,
                       sync_time,
                       stats_reset
                FROM pg_stat_checkpointer;
                """
            )
        else:
            payload["checkpointer"] = await postgresql_manager.execute_query(
                connection_id,
                """
                SELECT checkpoints_timed,
                       checkpoints_req,
                       checkpoint_write_time,
                       checkpoint_sync_time,
                       buffers_checkpoint
                FROM pg_stat_bgwriter;
                """
            )

        return text_result(payload, title="WAL & Checkpoint Activity")

    # 7️⃣ KAPASİTE / BOYUT RAPORU
    @mcp.tool(
        name="pg_capacity_report",
        description="Veritabanı ve büyük tabloların boyutlarını gösterir."
    )
    async def pg_capacity_report(
        ctx: Context,
        connection_id: int,
        top_tables: int = 20,
    ) -> ToolResult:
        db_sql = """
        SELECT
            datname,
            pg_database_size(datname) AS size_bytes
        FROM pg_database
        ORDER BY pg_database_size(datname) DESC;
        """
        tbl_sql = """
        SELECT
            schemaname,
            relname,
            pg_total_relation_size(format('%I.%I', schemaname, relname)) AS size_bytes
        FROM pg_stat_user_tables
        ORDER BY size_bytes DESC
        LIMIT $1;
        """
        db_sizes = await postgresql_manager.execute_query(connection_id, db_sql)
        tbl_sizes = await postgresql_manager.execute_query(connection_id, tbl_sql, top_tables)

        payload = {
            "databases": db_sizes,
            "top_tables": tbl_sizes,
        }
        return text_result(payload, title="PostgreSQL Capacity Report")

    # 8️⃣ REPLICATION STATUS
    @mcp.tool(
        name="pg_replication_status",
        description="Primary üzerinde pg_stat_replication view'i üzerinden standby ve replication lag detaylarını verir."
    )
    async def pg_replication_status(
        ctx: Context,
        connection_id: int,
    ) -> ToolResult:
        sql = """
        SELECT
            pid,
            usesysid,
            usename,
            application_name,
            client_addr,
            state,
            sync_state,
            write_lag,
            flush_lag,
            replay_lag,
            sent_lsn,
            write_lsn,
            flush_lsn,
            replay_lsn
        FROM pg_stat_replication;
        """
        rows = await postgresql_manager.execute_query(connection_id, sql)
        payload = {
            "replicas": rows,
            "note": "Bu rapor yalnızca primary/leader üzerinde anlamlıdır. Standby'da pg_stat_replication boş dönecektir."
        }
        return text_result(payload, title="Replication Status")
