# Methodology: Table Usage Analysis for AI Bot Context

## The Problem

A data platform with 1000+ tables across multiple catalogs (data lake, data warehouse, staging) needs an AI conversational layer. The bot can't load schema context for every table - it needs to know which tables matter most.

Naive approaches fail:
- Asking teams "what tables do you use?" produces incomplete, outdated lists
- Scanning dbt lineage only covers dbt-managed tables, missing ad-hoc queries
- Guessing based on table names misses actual usage patterns

## The Solution: Query Metadata Mining

Starburst Enterprise (Trino distribution) stores query metadata in `starburst_metadb`:

- `completed_queries` - every query that ran: user, timestamp, source client, duration, state
- `query_tables` - per-query table access: which catalog/schema/table was read or written, bytes/rows scanned
- `biac_role_grant` - role assignments per user

By joining these, we get ground-truth table usage without parsing SQL text.

## Why Not Parse SQL?

Regex-based SQL parsing is fragile:
- CTEs, subqueries, and lateral joins create ambiguous `FROM` clauses
- Quoted identifiers, catalog-qualified names, and aliases add complexity
- Views expand to base tables at execution time - the SQL text says `view_name` but the engine reads `base_table`

`query_tables` captures what the engine actually accessed, including view expansion. No parsing needed.

## Incremental Strategy

Query history is large (millions of rows over months). Full scans are expensive.

### sep_table_usage
- First run: 6-month backfill (~20 min)
- Daily: 7-day delta window
- FULL OUTER JOIN with existing rows so dormant tables still get `usage_status` aged
- Counts preserved from initial load (no double-counting risk)

### sep_table_access_by_user
- Watermark-based: reads from `MAX(last_accessed_ts)` of existing data
- Additive merge: new query counts are summed with existing
- No re-scan of old data

### sep_query_performance_daily
- 3-day re-scan window (percentiles can't be summed across runs)
- Merge replaces the overlapping days with fresh aggregates

## User Identity Resolution

Login emails don't always match HR records:
1. Legacy domain logins (e.g., `user@acquired-company.com` vs `user@parent.com`)
2. Nickname mismatches (`alexander.p` vs `alex.p`)
3. Service accounts (`sa_dbt_runner`)

Resolution chain:
1. Explicit alias table (manual overrides for known mismatches)
2. Domain bridge (try same handle at primary domain, verify against HR)
3. Direct email match
4. Handle fallback (`first.last` extracted from email, matched against HR)

## AI Bot Context Tiers

The priority score `(query_count * unique_users) / days_since_last_access` balances:
- Frequency: tables queried often are important
- Breadth: tables queried by many users are broadly relevant
- Recency: recently-accessed tables are more likely still relevant

| Tier | Tables | Bot Behavior |
|------|--------|--------------|
| Tier 1 | Top 50 | Full schema + descriptions always in context |
| Tier 2 | Top 200 | Schema loaded when user query matches keywords |
| Tier 3 | Remaining active | Table name only, suggested if relevant |
| Excluded | Stale/unused | Not indexed |

## Side Benefits

Beyond AI bot context, the same models enable:
- **Stale table cleanup** - identify tables nobody queries that still consume storage
- **Team usage dashboards** - which teams query what, useful for data ownership
- **Cost attribution** - bytes scanned per team/user for chargeback
- **Migration planning** - know which tables to prioritize when migrating platforms
