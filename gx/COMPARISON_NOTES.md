# Phase 2 Deliverable: dbt Tests vs Great Expectations — Comparison Notes

> Write your personal conclusions here after running both tools against the same
> ClickHouse marts. There are no "correct" answers — the goal is forming a concrete
> opinion based on what you actually observed.

---

## What I Ran

| Tool | Target | Run Command |
|---|---|---|
| dbt | `nyc_taxi_marts.*` | `dbt build` |
| Great Expectations | `fct_trips`, `dim_zones` | `great_expectations checkpoint run fct_trips_checkpoint` |

---

## Comparison: When dbt Tests Were Sufficient

> Fill in: which validations did dbt handle cleanly without needing GE at all?

Examples to consider:
- `not_null` on primary key columns
- `unique` on zone_id in dim_zones
- `relationships` between fct_trips.pickup_zone_id → dim_zones.zone_id
- `accepted_values` for payment_type

**My conclusion:**

_[Write here after running both tools]_

---

## Comparison: Where GE's Expressiveness Earned Its Complexity

> Fill in: which expectations required GE because dbt has no equivalent?

Expectations that go beyond what dbt can express:
- `expect_column_quantile_values_to_be_between` on `fare_amount` — dbt can't assert distributional statistics
- `expect_column_pair_values_A_to_be_greater_than_B` (dropoff > pickup) — dbt has no native cross-column comparison test
- `expect_table_row_count_to_be_between` with ±5% tolerance — dbt's singular tests use exact counts
- Dynamic bounds based on rolling statistics — dbt requires literal constants

**My conclusion:**

_[Write here after running both tools]_

---

## Comparison: Where GE Felt Like Overkill

> Fill in: situations where GE added overhead without proportional value over dbt.

Things to consider:
- Setup complexity (YAML, datasource config, checkpoints vs dbt schema.yml)
- Data Docs vs dbt docs — which gave you more useful information?
- Run time overhead per validation
- Maintenance burden when schema changes

**My conclusion:**

_[Write here after running both tools]_

---

## One-Line Verdict

> Complete this sentence after running both tools:
> "I would reach for Great Expectations instead of dbt tests when ___"

_[Write here]_

---

## Observations from the Data Docs Site

> After running `great_expectations docs build`, note what the Data Docs site showed
> that the dbt docs site didn't (or vice versa).

_[Write here]_
