# Starburst Table Usage Analysis

## Prerequisites

- Starburst Enterprise (or Trino with event listener) with `starburst_metadb` catalog
- dbt with `dbt-trino` adapter
- Access to `starburst_metadb.public.completed_queries` and `query_tables`
- (Optional) HR employee table for user enrichment
- (Optional) Elementary dbt package for `dbt_models` metadata

## Setup

1. Add the models to your dbt project under `models/marts/` (or your preferred path)
2. Update `schema.yml` with your owner email
3. Configure source references:
   - Replace `"your_catalog"."hr_schema"."employee_list"` with your HR table
   - Replace `"@legacy-domain.com"` in domain bridge with your actual legacy domains
   - Update `email_aliases` CTE with any known email mismatches
4. Run initial backfill (scans 6 months, ~20 min depending on query volume):

```bash
dbt run --select sep_table_usage sep_table_access_by_user sep_query_performance_daily
```

5. Schedule daily incremental runs (7-day delta, ~2 min):

```bash
dbt run --select sep_table_usage sep_table_access_by_user sep_query_performance_daily
```

## Usage

### Top tables for AI bot context

```bash
dbt compile --select top_tables_for_ai_bot
# Then run the compiled SQL in your Starburst client
```

Or query the materialized model directly:

```sql
SELECT catalog_name, schema_name, table_name, total_query_count, unique_users
FROM sep_table_usage
WHERE usage_status = 'active'
ORDER BY total_query_count DESC
LIMIT 50;
```

### Team usage breakdown

```sql
SELECT cost_center, catalog_name, schema_name, table_name, SUM(query_count) as queries
FROM sep_table_access_by_user
WHERE user_type = 'human'
GROUP BY 1, 2, 3, 4
ORDER BY queries DESC;
```

### Stale table candidates

```sql
SELECT catalog_name, schema_name, table_name, days_since_last_access, dbt_owner
FROM sep_table_usage
WHERE usage_status IN ('stale', 'unused') AND table_exists = true
ORDER BY days_since_last_access DESC;
```

## Adapting for Other Platforms

The core concept (mine query metadata to rank table usage) applies to any query engine:

| Platform | Metadata Source |
|----------|----------------|
| Starburst/Trino | `starburst_metadb.public.query_tables` |
| Snowflake | `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` |
| BigQuery | `INFORMATION_SCHEMA.JOBS` + referenced_tables |
| Databricks | `system.access.table_lineage` |
| Redshift | `STL_SCAN` + `SVL_QLOG` |
