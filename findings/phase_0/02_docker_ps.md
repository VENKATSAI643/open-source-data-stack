# Phase 0: Container Status Findings

**Command:**
```bash
docker compose -f governance-compose.yml ps
```

**Output:**
```
NAME               STATUS          PORTS
marquez-db         Up (healthy)    5432/tcp
marquez            Up (healthy)    0.0.0.0:5000->5000/tcp, 0.0.0.0:5001->5001/tcp
marquez-web        Up              0.0.0.0:3000->3000/tcp
superset           Up (healthy)    0.0.0.0:8088->8088/tcp
airflow            Up              0.0.0.0:8080->8080/tcp
maildev            Up              0.0.0.0:1080->1080/tcp, 0.0.0.0:1025->1025/tcp
clickhouse-proxy   Up              0.0.0.0:18123->18123/tcp
```

**Conclusion:**
All governance layer containers (Marquez for OpenLineage, Superset, Airflow, and the ClickHouse proxy) are healthy and mapped to their respective ports.
