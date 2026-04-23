-- Top tables for AI bot context selection.
-- Composite score: frequency x breadth x recency.
-- Run against the materialized sep_table_usage model.

SELECT
    catalog_name,
    schema_name,
    table_name,
    total_query_count,
    unique_users,
    read_count,
    write_count,
    days_since_last_access,
    usage_status,
    is_dbt_model,
    dbt_owner,
    dbt_path,
    -- Priority score: high query count + many users + recent access = high score
    ROUND(
        (total_query_count * unique_users)
        / CAST(GREATEST(days_since_last_access, 1) AS DOUBLE),
        2
    ) AS priority_score,
    -- Context tier recommendation
    CASE
        WHEN ROW_NUMBER() OVER (ORDER BY (total_query_count * unique_users) / CAST(GREATEST(days_since_last_access, 1) AS DOUBLE) DESC) <= 50
            THEN 'Tier 1 - Core (always loaded)'
        WHEN ROW_NUMBER() OVER (ORDER BY (total_query_count * unique_users) / CAST(GREATEST(days_since_last_access, 1) AS DOUBLE) DESC) <= 200
            THEN 'Tier 2 - Extended (on demand)'
        ELSE 'Tier 3 - Discovery (name only)'
    END AS ai_context_tier
FROM sep_table_usage
WHERE usage_status IN ('active', 'low_usage')
  AND table_exists = true
ORDER BY priority_score DESC
