# FEMA — quality patch for existing repository

This is an ADDITIVE patch for the project ZIP reviewed on 2026-11-16. It does not overwrite production Spark jobs, existing compose, data, or credentials. Extract into repo root. **Review files before commit.**

## Run on existing WSL

```
python3 -m pytest -q data_platform/tests/unit/test_source_contracts.py
bash infrastructure/testing/run_silver_readonly.sh
```

The second command mounts the EXISTING Iceberg warehouse read-only and produces `artifacts/test-reports/{silver-integrity.json,silver-integrity-junit.xml}`. It requires Docker and the Iceberg Maven jar. It DOES NOT test Debezium/Oracle/SQL Server end-to-end and is NOT an ephemeral full-stack CI. It uses a disposable Spark container but reads real persistent datasets, so run it in a quiet maintenance window and avoid overlapping table replacements.

## Before commit

```
git status --short
# Stage only reviewed paths, not git add -A yet.
git add data_platform/tests/unit/test_source_contracts.py data_platform/tests/integration/silver_integrity_readonly.py infrastructure/testing/
bash infrastructure/testing/precommit_audit.sh
```

Scan all tracked files and Git history for credentials separately. The ZIP had a plaintext Oracle/Debezium credential; rotate it, and do not commit its configuration. Rule out stray root files first. This scanner is only heuristic.

## Important producer risks, still outstanding

* Unified Bronze consumer uses a NEW checkpoint with `startingOffsets=latest`; it can **skip** Kafka records created before its initial start but after old consumers stopped. No safe handover until topic/partition offset coverage is reconciled. Bronze must maintain idempotence per `(topic,partition,offset)`.
* Source Silver code needs a real test involving `UPDATE`, `DELETE`, NULL key fallback and alternate Oracle NUMBER encodings. Fast static tests are **not** those functional tests.
* Source Silver `ROW_NUMBER` partitioning by source key across Kafka offsets assumes partition invariance, as in current deployment.
* Gold, Ceph, OLR regression and real full-stack ephemeral Compose remain separate subsequent milestones. Do not claim them complete in Bloc 2.
