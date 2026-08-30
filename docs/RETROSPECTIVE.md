# Project 5 Retrospective — Governance & Serving Layer

> Write this after completing all five phases.
> This is the synthesis deliverable — the whole point of the project distilled.

---

## 1. dbt Tests vs Great Expectations — My Actual Conclusion

> Not the textbook answer. What did YOU find after running both against real NYC taxi data?

**Where dbt won:**

_[Write here]_

**Where GE earned its place:**

_[Write here]_

**My rule of thumb going forward:**

_[Write here — e.g., "Use dbt for structural/relational validation, GE when you need distributional or cross-column checks"]_

---

## 2. What Marquez Showed Me That Logs Wouldn't Have

> After completing the bad-row incident trace in Phase 3, describe what the Marquez
> column-level lineage graph let you do that grep/log-tailing would not have.

**The bad row I injected:**

_[Describe: what column, what value, which file]_

**How long it took to trace back to the source in Marquez:**

_[Time estimate]_

**What I would have had to do to find it without lineage:**

_[Describe the manual process]_

**Concrete value of column-level lineage:**

_[Write here]_

---

## 3. Materialized View vs Raw Query — Timing Numbers

> From Dashboard 4: side-by-side comparison of `mv_daily_revenue` vs live `GROUP BY`.
> Fill in actual numbers from your Superset query duration display or ClickHouse EXPLAIN.

| Query Type | Query Duration | Rows Scanned |
|---|---|---|
| Live `GROUP BY` on `fct_trips` | ___ ms | ___ |
| `SELECT` from `mv_daily_revenue` | ___ ms | ___ |
| **Speedup factor** | ___ x | — |

**My interpretation of these numbers:**

_[Write here — does the speedup matter at this data scale? When would it matter?]_

---

## 4. What Surprised Me

> One thing that surprised you in each phase.

| Phase | Surprise |
|---|---|
| Phase 1 — dbt | _[Write here]_ |
| Phase 2 — GE | _[Write here]_ |
| Phase 3 — Marquez/OL | _[Write here]_ |
| Phase 4 — Superset | _[Write here]_ |

---

## 5. Environment Health — Final Reproducibility Check

Record the result of the clean teardown + restart test (Phase 5, Step 2):

- [ ] `docker compose -f governance-compose.yml down` completed cleanly
- [ ] All services came back up on restart
- [ ] `dbt build` passed after restart
- [ ] Marquez re-populated lineage after one DAG run

**Any flakiness observed:**

_[Write here — e.g., "Marquez web UI took 90s to initialize", "Superset forgot the ClickHouse connection after restart"]_
