{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['date', 'user_email', 'source', 'resource_group_id'],
        on_schema_change='sync_all_columns',
        tags=['data_monitoring']
    )
}}

-- Daily query-performance rollup. One row per (day, user, source, resource group).
-- Incremental re-does last 3 days (can't sum percentiles across runs).

WITH scan_window AS (
    SELECT
        query_id,
        create_time,
        CAST(create_time AS DATE)                        as day,
        usr,
        COALESCE(source, '<unknown>')                    as source,
        COALESCE(resource_group_id, '(unassigned)')      as resource_group_id,
        query_state,
        wall_time_ms,
        cpu_time_ms,
        queued_time_ms,
        peak_user_memory_bytes,
        peak_task_total_memory,
        completed_splits,
        total_bytes,
        output_bytes,
        total_rows
    FROM "starburst_metadb"."public"."completed_queries"
    WHERE 1=1
      {% if is_incremental() %}
      AND create_time >= CURRENT_TIMESTAMP - INTERVAL '3' DAY
      {% else %}
      AND create_time >= CURRENT_TIMESTAMP - INTERVAL '180' DAY
      {% endif %}
      AND usr IS NOT NULL
),

per_day AS (
    SELECT
        day                                              as date,
        LOWER(usr)                                       as user_email,
        source,
        resource_group_id,

        COUNT(DISTINCT query_id)                                   as query_count,
        SUM(CASE WHEN query_state = 'FINISHED' THEN 1 ELSE 0 END)  as successful_count,
        SUM(CASE WHEN query_state = 'FAILED'   THEN 1 ELSE 0 END)  as failed_count,

        SUM(COALESCE(wall_time_ms,   0))                 as sum_wall_ms,
        SUM(COALESCE(cpu_time_ms,    0))                 as sum_cpu_ms,
        SUM(COALESCE(queued_time_ms, 0))                 as sum_queued_ms,
        SUM(COALESCE(total_bytes,    0))                 as sum_bytes_scanned,
        SUM(COALESCE(output_bytes,   0))                 as sum_output_bytes,
        SUM(COALESCE(total_rows,     0))                 as sum_rows_scanned,

        MAX(wall_time_ms)                                as max_wall_ms,
        MAX(peak_user_memory_bytes)                      as max_mem_bytes,
        MAX(peak_task_total_memory)                      as max_task_mem_bytes,
        MAX(queued_time_ms)                              as max_queued_ms,
        SUM(COALESCE(completed_splits, 0))               as sum_splits,
        MAX(completed_splits)                            as max_splits,

        APPROX_PERCENTILE(wall_time_ms,           0.50)  as p50_wall_ms,
        APPROX_PERCENTILE(wall_time_ms,           0.95)  as p95_wall_ms,
        APPROX_PERCENTILE(wall_time_ms,           0.99)  as p99_wall_ms,
        APPROX_PERCENTILE(peak_user_memory_bytes, 0.50)  as p50_mem_bytes,
        APPROX_PERCENTILE(peak_user_memory_bytes, 0.95)  as p95_mem_bytes,
        APPROX_PERCENTILE(peak_user_memory_bytes, 0.99)  as p99_mem_bytes,
        APPROX_PERCENTILE(queued_time_ms,         0.95)  as p95_queued_ms,

        SUM(CASE WHEN queued_time_ms > 1000  THEN 1 ELSE 0 END) as queries_queued_over_1s,
        SUM(CASE WHEN queued_time_ms > 10000 THEN 1 ELSE 0 END) as queries_queued_over_10s,

        MIN(create_time)                                 as first_seen_ts,
        MAX(create_time)                                 as last_seen_ts
    FROM scan_window
    GROUP BY 1, 2, 3, 4
),

employees_raw AS (
    SELECT
        LOWER(primary_work_email)                        as email,
        LOWER(first_name) || '.' || LOWER(last_name)     as handle,
        full_name,
        job_title,
        cost_center,
        cost_center_hierarchy,
        active_status
    FROM "your_catalog"."hr_schema"."employee_list"
    WHERE _fivetran_deleted = false
      AND first_name IS NOT NULL
      AND last_name  IS NOT NULL
),

emp_by_email AS (
    SELECT email, full_name, job_title, cost_center, cost_center_hierarchy
    FROM (
        SELECT e.*,
               ROW_NUMBER() OVER (
                   PARTITION BY email
                   ORDER BY CASE WHEN active_status = 1 THEN 0 ELSE 1 END
               ) as rn
        FROM employees_raw e
        WHERE email IS NOT NULL
    )
    WHERE rn = 1
),

emp_by_handle AS (
    SELECT handle, full_name, job_title, cost_center, cost_center_hierarchy
    FROM (
        SELECT e.*,
               ROW_NUMBER() OVER (
                   PARTITION BY handle
                   ORDER BY CASE WHEN active_status = 1 THEN 0 ELSE 1 END
               ) as rn
        FROM employees_raw e
    )
    WHERE rn = 1
),

user_roles AS (
    SELECT
        LOWER(rg.subject_user) as user_email,
        ARRAY_AGG(DISTINCT r.name) as starburst_roles
    FROM "starburst_metadb"."public"."biac_role_grant" rg
    JOIN "starburst_metadb"."public"."biac_role" r ON r.id = rg.role_id
    WHERE rg.subject_user IS NOT NULL
    GROUP BY LOWER(rg.subject_user)
)

SELECT
    p.date,
    p.user_email,
    p.source,
    p.resource_group_id,
    p.query_count,
    p.successful_count,
    p.failed_count,
    p.sum_wall_ms,
    p.sum_cpu_ms,
    p.sum_queued_ms,
    p.sum_bytes_scanned,
    p.sum_output_bytes,
    p.sum_rows_scanned,
    p.max_wall_ms,
    p.max_mem_bytes,
    p.max_task_mem_bytes,
    p.max_queued_ms,
    p.sum_splits,
    p.max_splits,
    p.p50_wall_ms,
    p.p95_wall_ms,
    p.p99_wall_ms,
    p.p50_mem_bytes,
    p.p95_mem_bytes,
    p.p99_mem_bytes,
    p.p95_queued_ms,
    p.queries_queued_over_1s,
    p.queries_queued_over_10s,
    p.first_seen_ts,
    p.last_seen_ts,

    CASE
        WHEN starts_with(p.user_email, 'sa_')   THEN 'service_account'
        WHEN p.user_email = '<sep-system-user>' THEN 'system'
        ELSE 'human'
    END as user_type,

    COALESCE(e_direct.full_name,             e_fallback.full_name)             as employee_name,
    COALESCE(e_direct.job_title,             e_fallback.job_title)             as job_title,
    COALESCE(e_direct.cost_center,           e_fallback.cost_center)           as cost_center,
    COALESCE(e_direct.cost_center_hierarchy, e_fallback.cost_center_hierarchy) as cost_center_hierarchy,

    ur.starburst_roles

FROM per_day p
LEFT JOIN emp_by_email e_direct
    ON e_direct.email = p.user_email
LEFT JOIN emp_by_handle e_fallback
    ON e_direct.email IS NULL
   AND e_fallback.handle = SPLIT_PART(p.user_email, '@', 1)
LEFT JOIN user_roles ur
    ON ur.user_email = p.user_email
