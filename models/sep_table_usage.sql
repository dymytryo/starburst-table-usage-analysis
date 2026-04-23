{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['catalog_name', 'schema_name', 'table_name'],
        on_schema_change='sync_all_columns',
        tags=['data_monitoring']
    )
}}

-- Table-level usage aggregation from Starburst query metadata.
-- First run: 6-month backfill. Incremental: 7-day delta + FULL OUTER JOIN
-- to age dormant tables correctly.

WITH scan_window AS (
    SELECT query_id, create_time, usr
    FROM "starburst_metadb"."public"."completed_queries"
    WHERE query_state = 'FINISHED'
      {% if is_incremental() %}
      AND create_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
      {% else %}
      AND create_time >= CURRENT_TIMESTAMP - INTERVAL '180' DAY
      {% endif %}
),

table_access AS (
    SELECT
        qt.catalog_name,
        qt.schema_name,
        qt.table_name,
        CAST(cq.create_time AS DATE) as query_date,
        cq.usr                       as user_email,
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
        MIN(query_date)                                    as first_accessed,
        MAX(query_date)                                    as last_accessed,
        COUNT(DISTINCT query_id)                           as total_query_count,
        COUNT(DISTINCT user_email)                         as unique_users,
        SUM(CASE WHEN is_output THEN 1 ELSE 0 END)        as write_count,
        SUM(CASE WHEN NOT is_output THEN 1 ELSE 0 END)    as read_count,
        SUM(COALESCE(physical_bytes, 0))                   as total_bytes_scanned,
        SUM(COALESCE(physical_rows, 0))                    as total_rows_scanned
    FROM table_access
    GROUP BY catalog_name, schema_name, table_name
),

merged AS (
    SELECT
        {% if is_incremental() %}
        COALESCE(n.catalog_name, p.catalog_name)     as catalog_name,
        COALESCE(n.schema_name, p.schema_name)       as schema_name,
        COALESCE(n.table_name, p.table_name)         as table_name,
        COALESCE(LEAST(n.first_accessed, p.first_accessed), n.first_accessed, p.first_accessed)       as first_accessed,
        COALESCE(GREATEST(n.last_accessed, p.last_accessed), n.last_accessed, p.last_accessed)        as last_accessed,
        COALESCE(p.total_query_count, n.total_query_count) as total_query_count,
        COALESCE(p.unique_users, n.unique_users)           as unique_users,
        COALESCE(p.write_count, n.write_count)             as write_count,
        COALESCE(p.read_count, n.read_count)               as read_count,
        COALESCE(p.total_bytes_scanned, n.total_bytes_scanned) as total_bytes_scanned,
        COALESCE(p.total_rows_scanned, n.total_rows_scanned)  as total_rows_scanned,
        p.removed_at as prev_removed_at
        {% else %}
        n.catalog_name,
        n.schema_name,
        n.table_name,
        n.first_accessed,
        n.last_accessed,
        n.total_query_count,
        n.unique_users,
        n.write_count,
        n.read_count,
        n.total_bytes_scanned,
        n.total_rows_scanned,
        CAST(NULL AS DATE) as prev_removed_at
        {% endif %}
    FROM new_activity n
    {% if is_incremental() %}
    FULL OUTER JOIN {{ this }} p
        ON n.catalog_name = p.catalog_name
        AND n.schema_name = p.schema_name
        AND n.table_name = p.table_name
    {% endif %}
),

-- Verify tables still exist (only catalogs we have access to)
existing_tables AS (
    SELECT DISTINCT table_catalog as catalog_name, table_schema as schema_name, table_name
    FROM bdc_redshift.information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
    UNION
    SELECT DISTINCT table_catalog, table_schema, table_name
    FROM bdc_lakehouse.information_schema.tables
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
),

dbt_models_deduped AS (
    SELECT database_name, schema_name, alias,
           unique_id, materialization, owner, tags, path, package_name
    FROM (
        SELECT dm.*,
               ROW_NUMBER() OVER (
                   PARTITION BY LOWER(database_name), LOWER(schema_name), LOWER(alias)
                   ORDER BY generated_at DESC
               ) as rn
        FROM {{ ref('dbt_models') }} dm
    )
    WHERE rn = 1
)

SELECT
    m.catalog_name,
    m.schema_name,
    m.table_name,
    m.first_accessed,
    m.last_accessed,
    m.total_query_count,
    m.unique_users,
    m.write_count,
    m.read_count,
    m.total_bytes_scanned,
    m.total_rows_scanned,

    DATE_DIFF('day', m.last_accessed, CURRENT_DATE) as days_since_last_access,
    CASE
        WHEN DATE_DIFF('day', m.last_accessed, CURRENT_DATE) > 180 THEN 'stale'
        WHEN DATE_DIFF('day', m.last_accessed, CURRENT_DATE) > 90  THEN 'unused'
        WHEN DATE_DIFF('day', m.last_accessed, CURRENT_DATE) > 30  THEN 'low_usage'
        ELSE 'active'
    END as usage_status,

    et.catalog_name IS NOT NULL as table_exists,
    CASE
        WHEN m.catalog_name NOT IN ('bdc_redshift', 'bdc_lakehouse') THEN NULL
        WHEN et.catalog_name IS NOT NULL THEN NULL
        WHEN m.prev_removed_at IS NOT NULL THEN m.prev_removed_at
        ELSE CURRENT_DATE
    END as removed_at,

    dm.unique_id IS NOT NULL   as is_dbt_model,
    dm.unique_id               as dbt_unique_id,
    dm.materialization         as dbt_materialization,
    dm.owner                   as dbt_owner,
    dm.tags                    as dbt_tags,
    dm.path                    as dbt_path,
    dm.package_name            as dbt_package

FROM merged m
LEFT JOIN existing_tables et
    ON LOWER(et.catalog_name) = LOWER(m.catalog_name)
    AND LOWER(et.schema_name) = LOWER(m.schema_name)
    AND LOWER(et.table_name)  = LOWER(m.table_name)
LEFT JOIN dbt_models_deduped dm
    ON LOWER(dm.database_name) = LOWER(m.catalog_name)
    AND LOWER(dm.schema_name)  = LOWER(m.schema_name)
    AND LOWER(dm.alias)        = LOWER(m.table_name)
