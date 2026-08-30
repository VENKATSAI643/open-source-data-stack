# Phase 2: Great Expectations Validation Findings

**Command:**
```bash
python3 run_ge.py
```

**Output:**
```
Datasource added.
Suite saved.
Running checkpoint...
Calculating Metrics: 100%|█████████████████████████████████████████████████████████████| 22/22 [00:00<00:00, 96.27it/s]
Success: False
Data Docs built.
```

**Conclusion:**
Great Expectations successfully connected to ClickHouse via `clickhouse+native://`. The checkpoint evaluated to `Success: False`, as our suite `trips_physics_suite` (which asserts `trip_distance` < 200 miles and `trip_duration_min` < 500) successfully caught the out-of-range data anomalies in the `fct_trips` mart. The detailed results have been rendered to Data Docs located at `gx/uncommitted/data_docs/local_site/index.html`.
