-- AI Bot Context Recommendation
--
-- Combines table usage, user access patterns, and query performance
-- into a single prioritized output for configuring an AI "Talk to the Data" bot.
--
-- Output: one row per table with top consumers, query source
-- breakdown, and performance characteristics.

WITH ranked_tables AS (
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
        table_exists,
        ROUND(
            CAST(total_query_count * unique_users AS DOUBLE)
            / GREATEST(days_since_last_access, 1),
            1
        ) AS priority_score
    FROM sep_table_usage
    WHERE usage_status IN ('active', 'low_usage')
      AND table_exists = true
),

-- Top 3 users per table (who would the bot be serving?)
top_users AS (
    SELECT
        catalog_name, schema_name, table_name,
        user_email, employee_name, cost_center, query_count,
        ROW_NUMBER() OVER (
            PARTITION BY catalog_name, schema_name, table_name
            ORDER BY query_count DESC
        ) AS user_rank
    FROM sep_table_access_by_user
    WHERE user_type = 'human'
),

user_summary AS (
    SELECT
        catalog_name, schema_name, table_name,
        ARRAY_AGG(employee_name ORDER BY user_rank) AS top_users,
        ARRAY_AGG(DISTINCT cost_center)             AS teams
    FROM top_users
    WHERE user_rank <= 3
    GROUP BY 1, 2, 3
),

-- Query source breakdown (how are people accessing this table?)
source_breakdown AS (
    SELECT
        catalog_name, schema_name, table_name,
        source,
        SUM(query_count) AS queries_from_source
    FROM sep_table_access_by_user
    GROUP BY 1, 2, 3, 4
),

top_source AS (
    SELECT
        catalog_name, schema_name, table_name,
        source AS primary_source,
        queries_from_source
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY catalog_name, schema_name, table_name
                ORDER BY queries_from_source DESC
            ) AS rn
        FROM source_breakdown
    )
    WHERE rn = 1
)

SELECT
    rt.catalog_name,
    rt.schema_name,
    rt.table_name,
    rt.priority_score,

    -- Usage stats
    rt.total_query_count,
    rt.unique_users,
    rt.read_count,
    rt.write_count,
    rt.days_since_last_access,

    -- Who uses it
    us.top_users,
    us.teams,

    -- How it's accessed
    ts.primary_source,

    -- Metadata
    rt.is_dbt_model,
    rt.dbt_owner,
    rt.dbt_path

FROM ranked_tables rt
LEFT JOIN user_summary us
    ON us.catalog_name = rt.catalog_name
   AND us.schema_name  = rt.schema_name
   AND us.table_name   = rt.table_name
LEFT JOIN top_source ts
    ON ts.catalog_name = rt.catalog_name
   AND ts.schema_name  = rt.schema_name
   AND ts.table_name   = rt.table_name
ORDER BY rt.priority_score DESC
