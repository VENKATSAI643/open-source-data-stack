# Phase 1: dbt Modeling Findings

## 1. dbt run

**Command:**
```bash
dbt run
```

**Output:**
```
14:52:56  Finished running 4 table models, 3 view models in 0 hours 0 minutes and 13.61 seconds (13.61s).
14:52:56  
14:52:56  Completed successfully
14:52:56  
14:52:56  Done. PASS=7 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=7
```

**Conclusion:**
All models (1 seed, 3 views, 4 tables) were successfully instantiated in the ClickHouse `nyc_taxi_marts` database without errors.

---

## 2. dbt test

**Command:**
```bash
dbt test
```

**Output:**
```
14:53:31  Finished running 24 data tests in 0 hours 0 minutes and 5.98 seconds (5.98s).
14:53:31  
14:53:31  Completed with 2 warnings:
14:53:31  
14:53:31  [WARNING]: in test assert_trip_distance_plausible (tests/assert_trip_distance_plausible.sql)
14:53:31  [WARNING]: Got 159648 results, configured to warn if != 0
14:53:31  
14:53:31    compiled code at target/compiled/nyc_taxi_dbt/tests/assert_trip_distance_plausible.sql
14:53:31  
14:53:31  [WARNING]: in test assert_trip_physics (tests/assert_trip_physics.sql)
14:53:31  [WARNING]: Got 653 results, configured to warn if != 0
14:53:31  
14:53:31    compiled code at target/compiled/nyc_taxi_dbt/tests/assert_trip_physics.sql
14:53:31  
14:53:31  Done. PASS=22 WARN=2 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=24
```

**Conclusion:**
Testing executed successfully. 22 tests passed completely. As expected due to real-world anomalies in the NYC taxi data, `assert_trip_distance_plausible` and `assert_trip_physics` appropriately flagged records with warnings. No structural testing errors occurred.
