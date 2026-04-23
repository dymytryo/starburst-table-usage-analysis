-- Tables that are candidates for cleanup or deprecation.
-- Stale/unused tables that still exist but nobody queries.

SELECT
    catalog_name,
    schema_name,
    table_name,
    usage_status,
    days_since_last_access,
    last_accessed,
    total_query_count,
    unique_users,
    is_dbt_model,
    dbt_owner,
    dbt_path,
    table_exists,
    removed_at
FROM sep_table_usage
WHERE usage_status IN ('stale', 'unused')
  AND table_exists = true
ORDER BY days_since_last_access DESC
