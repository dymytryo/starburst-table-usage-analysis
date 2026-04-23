{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['catalog_name', 'schema_name', 'table_name', 'user_email', 'source'],
        on_schema_change='sync_all_columns',
        tags=['data_monitoring']
    )
}}

-- Per (catalog, schema, table, user, source) access rollup with HR +
-- Starburst-role enrichment.

WITH scan_window AS (
    SELECT query_id, create_time, usr, source
    FROM "starburst_metadb"."public"."completed_queries"
    WHERE query_state = 'FINISHED'
      {% if is_incremental() %}
      AND create_time > (SELECT COALESCE(MAX(last_accessed_ts), CURRENT_TIMESTAMP - INTERVAL '180' DAY) FROM {{ this }})
      {% else %}
      AND create_time >= CURRENT_TIMESTAMP - INTERVAL '180' DAY
      {% endif %}
),

table_access AS (
    SELECT
        qt.catalog_name,
        qt.schema_name,
        qt.table_name,
        cq.create_time,
        LOWER(cq.usr)                    as user_email,
        COALESCE(cq.source, '<unknown>') as source,
        qt.is_output,
        qt.physical_bytes,
        qt.physical_rows,
        cq.query_id
    FROM "starburst_metadb"."public"."query_tables" qt
    INNER JOIN scan_window cq ON qt.query_id = cq.query_id
    WHERE qt.catalog_name NOT IN ('system', 'jmx', 'tpch', 'tpcds', 'starburst_metadb')
      AND qt.schema_name != 'information_schema'
      AND qt.table_name NOT LIKE 'tmp_%'
      AND qt.table_name NOT LIKE '%__dbt_tmp%'
      AND qt.table_name NOT LIKE '%$%'
),

new_activity AS (
    SELECT
        catalog_name,
        schema_name,
        table_name,
        user_email,
        source,
        CAST(MIN(create_time) AS DATE)                  as first_accessed,
        CAST(MAX(create_time) AS DATE)                  as last_accessed,
        MAX(create_time)                                as last_accessed_ts,
        COUNT(DISTINCT query_id)                        as query_count,
        SUM(CASE WHEN is_output THEN 1 ELSE 0 END)     as write_count,
        SUM(CASE WHEN NOT is_output THEN 1 ELSE 0 END) as read_count,
        SUM(COALESCE(physical_bytes, 0))                as bytes_scanned,
        SUM(COALESCE(physical_rows, 0))                 as rows_scanned
    FROM table_access
    GROUP BY 1, 2, 3, 4, 5
),

merged AS (
    {% if is_incremental() %}
    SELECT
        n.catalog_name,
        n.schema_name,
        n.table_name,
        n.user_email,
        n.source,
        COALESCE(LEAST(p.first_accessed, n.first_accessed), n.first_accessed)     as first_accessed,
        GREATEST(COALESCE(p.last_accessed, n.last_accessed), n.last_accessed)     as last_accessed,
        GREATEST(COALESCE(p.last_accessed_ts, n.last_accessed_ts), n.last_accessed_ts) as last_accessed_ts,
        COALESCE(p.query_count, 0)   + n.query_count   as query_count,
        COALESCE(p.write_count, 0)   + n.write_count   as write_count,
        COALESCE(p.read_count, 0)    + n.read_count    as read_count,
        COALESCE(p.bytes_scanned, 0) + n.bytes_scanned as bytes_scanned,
        COALESCE(p.rows_scanned, 0)  + n.rows_scanned  as rows_scanned
    FROM new_activity n
    LEFT JOIN {{ this }} p
        ON p.catalog_name = n.catalog_name
       AND p.schema_name  = n.schema_name
       AND p.table_name   = n.table_name
       AND p.user_email   = n.user_email
       AND p.source       = n.source
    {% else %}
    SELECT *
    FROM new_activity
    {% endif %}
),

-- Manual login -> HR overrides (nicknames, name changes).
-- Domain swaps (e.g. @divvypay.com -> @hq.bill.com) are handled automatically.
email_aliases AS (
    SELECT login_email, hr_email FROM (VALUES
        ('alexander.proctor@example.com', 'alex.proctor@example.com')
    ) AS t(login_email, hr_email)
),

employees_raw AS (
    SELECT
        LOWER(primary_work_email)                            as email,
        LOWER(first_name) || '.' || LOWER(last_name)         as handle,
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
),

-- For legacy domain logins, try the same handle at the primary domain.
domain_bridge AS (
    SELECT DISTINCT
        m.user_email                                             as login_email,
        SPLIT_PART(m.user_email, '@', 1) || '@example.com'       as candidate_email
    FROM merged m
    WHERE m.user_email LIKE '%@legacy-domain.com'
),

domain_bridge_matches AS (
    SELECT db.login_email, db.candidate_email as hr_email
    FROM domain_bridge db
    INNER JOIN emp_by_email e ON e.email = db.candidate_email
),

resolved AS (
    SELECT
        m.*,
        COALESCE(ea.hr_email, db.hr_email, m.user_email) as hr_email
    FROM merged m
    LEFT JOIN email_aliases         ea ON ea.login_email = m.user_email
    LEFT JOIN domain_bridge_matches db ON db.login_email = m.user_email
)

SELECT
    r.catalog_name,
    r.schema_name,
    r.table_name,
    r.user_email,
    r.source,
    r.first_accessed,
    r.last_accessed,
    r.last_accessed_ts,
    r.query_count,
    r.write_count,
    r.read_count,
    r.bytes_scanned,
    r.rows_scanned,

    -- starts_with avoids LIKE wildcard issue: 'sa_%' would match sam./sarah.
    CASE
        WHEN starts_with(r.user_email, 'sa_')   THEN 'service_account'
        WHEN r.user_email = '<sep-system-user>' THEN 'system'
        ELSE 'human'
    END as user_type,

    COALESCE(e_direct.full_name, e_fallback.full_name)                         as employee_name,
    COALESCE(e_direct.job_title, e_fallback.job_title)                         as job_title,
    COALESCE(e_direct.cost_center, e_fallback.cost_center)                     as cost_center,
    COALESCE(e_direct.cost_center_hierarchy, e_fallback.cost_center_hierarchy) as cost_center_hierarchy,

    ur.starburst_roles

FROM resolved r
LEFT JOIN emp_by_email e_direct
    ON e_direct.email = r.hr_email
LEFT JOIN emp_by_handle e_fallback
    ON e_direct.email IS NULL
   AND e_fallback.handle = SPLIT_PART(r.hr_email, '@', 1)
LEFT JOIN user_roles ur
    ON ur.user_email = r.user_email
