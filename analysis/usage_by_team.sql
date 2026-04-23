-- Usage breakdown by team (cost center).
-- Shows which teams are heaviest consumers and what they query.
-- Useful for building team-specific AI bot configurations.

WITH team_usage AS (
    SELECT
        cost_center,
        catalog_name,
        schema_name,
        table_name,
        SUM(query_count)    as total_queries,
        COUNT(DISTINCT user_email) as team_members_querying,
        MAX(last_accessed)  as last_team_access
    FROM sep_table_access_by_user
    WHERE user_type = 'human'
      AND cost_center IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    cost_center,
    catalog_name,
    schema_name,
    table_name,
    total_queries,
    team_members_querying,
    last_team_access,
    ROW_NUMBER() OVER (
        PARTITION BY cost_center
        ORDER BY total_queries DESC
    ) as rank_within_team
FROM team_usage
QUALIFY rank_within_team <= 20
ORDER BY cost_center, rank_within_team
