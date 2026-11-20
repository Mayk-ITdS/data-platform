# FEMA CI Phase 2 — Kafka -> actual Bronze -> actual canonical Silver

Requires Phase 1 scripts already installed in repository and Docker Compose plugin.
Run in Ubuntu-Test from repo root:

    unzip -n fema-ci-phase2-kafka.zip
    chmod +x infrastructure/testing/run_kafka_e2e.sh
    bash infrastructure/testing/run_kafka_e2e.sh
    echo "Exit code: $?"

The runner:
1. Starts an **isolated** Redpanda broker (Kafka API compatible) in its own Compose project.
2. Publishes 10 Debezium-shaped events on four real production topic names.
3. Creates an isolated Iceberg Bronze table with the production schema.
4. Runs your existing `data_platform/spark/bronze_ingestion.py` unchanged.
5. Runs your existing `data_platform/spark/canonical_policy_claim.py` unchanged.
6. Checks eight assertions and saves JUnit XML/JSON in artifacts/test-reports.
7. Removes test-only container/network/volume via trap, even on failures.

Scope: no Oracle or SQL Server service and no actual Debezium connector. This is a
**Kafka -> Bronze -> canonical Policy/Claim Silver** integration test, not a full
source-database-to-lakehouse E2E. It does NOT check all Silver extension tables.

Known limits: transient Maven/network dependency failures or WSL memory limits may
require retry / tuning. This package was syntax-reviewed, not executed against Docker
in the preparation environment. Do NOT run `docker compose down` on your production stack.

Does not overwrite previous compose.test.yml or run_tests.sh. Does not require
access to `infrastructure/volumes` or `fema-cdc` network. Reports survive teardown.

Current canonical Silver logic assumes one Kafka partition per topic. Test uses one.
The log confirms real Kafka ingestion rather than a mock/reducer.
