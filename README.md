# Starburst Table Usage Analysis

Identifying the most-accessed tables in a data lake/warehouse to inform context selection for an AI "Talk to the Data" assistant.

## Problem

When building a conversational AI layer over a large data platform (1000+ tables across multiple catalogs), the bot can't have context on everything. It needs to know which tables matter most - the ones analysts actually query - so it can prioritize schema knowledge, suggest relevant joins, and answer questions accurately.

## Approach

Instead of parsing raw SQL query logs with regex, this project leverages Starburst Enterprise's built-in query metadata (`starburst_metadb`) to build three incremental dbt models that answer:

1. **Which tables are most used?** - `sep_table_usage` ranks every table by query count, unique users, read/write split, and freshness
2. **Who uses what?** - `sep_table_access_by_user` breaks usage down by user, source (CLI/dbt/Tableau), and enriches with HR data (team, cost center)
3. **What are the performance patterns?** - `sep_query_performance_daily` tracks daily query volume, latency percentiles, and resource consumption per user

The output feeds a prioritized table list for the AI bot's context window.

## Architecture

```
starburst_metadb.completed_queries ──┐
                                     ├──> sep_table_usage ──────────────> AI Bot Context Config
starburst_metadb.query_tables ───────┘         │
                                               ├──> Priority ranking
elementary.dbt_models ─────────────────────────┘    (query count x unique users x recency)
                                               
starburst_metadb.completed_queries ──┐
                                     ├──> sep_table_access_by_user ──> User/team analysis
HR employee list ────────────────────┘
starburst_metadb.biac_role_grant ────┘

starburst_metadb.completed_queries ──> sep_query_performance_daily ──> Cost/performance insights
```

## Data Sources

| Source | Description |
|--------|-------------|
| `starburst_metadb.public.completed_queries` | Every finished query with timing, user, source |
| `starburst_metadb.public.query_tables` | Per-query table access (catalog, schema, table, read/write, bytes) |
| `starburst_metadb.public.biac_role_grant` | Starburst role assignments per user |
| `bdc_lakehouse.se_other.current_bill_employee_list` | HR data for user enrichment |
| `elementary.dbt_models` | dbt model metadata (owner, path, materialization) |
| `information_schema` (per catalog) | Table existence verification |

## Models

### sep_table_usage

Incremental merge model. One row per (catalog, schema, table).

- First run scans 6 months of history
- Daily runs scan 7 days + refresh all existing rows via FULL OUTER JOIN
- Classifies tables as `active` / `low_usage` / `unused` / `stale` based on days since last access
- Checks `information_schema` to detect dropped tables
- Enriches with dbt metadata (owner, path, materialization)

Key columns: `total_query_count`, `unique_users`, `read_count`, `write_count`, `days_since_last_access`, `usage_status`, `table_exists`, `dbt_owner`

### sep_table_access_by_user

Incremental merge model. One row per (catalog, schema, table, user, source).

- Resolves login emails to HR identities (handles domain aliases like @divvypay.com -> @hq.bill.com)
- Classifies users as `human` / `service_account` / `system`
- Enriches with employee name, job title, cost center, Starburst roles

Key columns: `query_count`, `read_count`, `write_count`, `bytes_scanned`, `user_type`, `employee_name`, `cost_center`, `starburst_roles`

### sep_query_performance_daily

Incremental merge model. One row per (date, user, source, resource_group).

- Tracks latency percentiles (p50/p95/p99) for wall time, memory, queue time
- Counts queries queued >1s and >10s (capacity pressure signals)
- Same HR enrichment as `sep_table_access_by_user`

## AI Bot Context Selection

The analysis output produces a prioritized table list using a composite score:

```sql
SELECT
    catalog_name,
    schema_name,
    table_name,
    total_query_count,
    unique_users,
    days_since_last_access,
    usage_status,
    dbt_owner,
    -- Composite score: frequency x breadth x recency
    (total_query_count * unique_users) / GREATEST(days_since_last_access, 1) AS priority_score
FROM sep_table_usage
WHERE usage_status = 'active'
ORDER BY priority_score DESC
```

Tables with high priority scores should be included in the bot's schema context. The `sep_table_access_by_user` model further helps identify which teams query which tables, enabling team-specific bot configurations.

### Recommended Bot Context Tiers

| Tier | Criteria | Bot Behavior |
|------|----------|--------------|
| Tier 1 (Core) | Top 50 by priority_score | Full schema + sample data + column descriptions always loaded |
| Tier 2 (Extended) | Top 200 active tables | Schema loaded on demand when user query matches |
| Tier 3 (Discovery) | Remaining active tables | Table name + description only, suggest to user if relevant |
| Excluded | stale/unused tables | Not indexed |

## Project Structure

```
starburst-table-usage-analysis/
  README.md                          # This file
  models/
    sep_table_usage.sql              # Table-level usage aggregation
    sep_table_access_by_user.sql     # Per-user table access breakdown
    sep_query_performance_daily.sql  # Daily query performance metrics
    schema.yml                       # Column documentation
  analysis/
    top_tables_for_ai_bot.sql        # Priority ranking query
    usage_by_team.sql                # Team-level usage breakdown
    stale_table_candidates.sql       # Cleanup candidates
  docs/
    approach.md                      # Detailed methodology
```

## Key Design Decisions

1. **Incremental merge over full refresh** - Query history is large (millions of rows over 6 months). Incremental runs process only the delta window (7 days for usage, 3 days for performance) while keeping historical aggregates accurate.

2. **FULL OUTER JOIN on incremental** - Tables with no recent activity still need their `usage_status` and `days_since_last_access` updated. A LEFT JOIN from new activity would miss dormant tables.

3. **HR enrichment at query time** - Login emails don't always match HR emails (domain aliases, nicknames). The model resolves these with a priority chain: explicit alias -> domain bridge -> direct match -> handle fallback.

4. **`starts_with` over `LIKE` for service accounts** - Service accounts are prefixed `sa_`. Using `LIKE 'sa_%'` would match `sam.`, `sarah.`, etc. because `_` is a LIKE wildcard. `starts_with()` avoids this.

5. **Separate existence check** - Tables can appear in query history but no longer exist (dropped). Checking `information_schema` catches these so the bot doesn't try to query phantom tables.

## Tech Stack

- Starburst Enterprise (Trino) - Query engine and metadata source
- dbt (dbt-trino adapter) - Model orchestration and incremental logic
- S3 + Glue Catalog - Underlying storage (Iceberg tables)
- Elementary - dbt observability (provides `dbt_models` metadata)
