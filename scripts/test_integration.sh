#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")/.."

ROOT="$PWD"
RED_DIR="$(mktemp -d "$ROOT/.fema_red_XXXXXX")"
LOG_DIR="$(mktemp -d /tmp/fema-test-results.XXXXXX)"

cleanup() {
    rm -rf -- "$RED_DIR"
}
trap cleanup EXIT

echo "Test logs: $LOG_DIR"

echo "=== 1/4 STATIC CHECKS ==="

python - <<'PY'
import ast
from pathlib import Path

paths = [
    "data_platform/spark/canonical_silver_extensions.py",
    "data_platform/spark/holder_identity.py",
    "tests/spark/test_cdc_alias_regression.py",
    "tests/spark/test_holder_identity_integration.py",
]

for filename in paths:
    ast.parse(Path(filename).read_text(), filename=filename)
    print("PASS syntax:", filename)
PY

git diff --check
echo "PASS static checks"

export RED_DIR

echo "=== PREPARE CONTROLLED RED ==="

python - <<'PY'
import os
from pathlib import Path

root = Path(os.environ["RED_DIR"])
source = Path("data_platform/spark/canonical_silver_extensions.py")
test = Path("tests/spark/test_cdc_alias_regression.py")

code = source.read_text()

correct = '''                value = F.coalesce(
                    F.get_json_object(
                        F.col("record_json"),
                        f"$.AP_{suffix}"
                    ),
                    F.get_json_object(
                        F.col("record_json"),
                        f"$.AB_{suffix}"
                    )
                )'''

defective = '''                value = F.get_json_object(
                    F.col("record_json"),
                    f"$.AB_{suffix}"
                )'''

if code.count(correct) != 1:
    raise RuntimeError("Cannot identify production AB/AP mapping")

(root / "broken_silver.py").write_text(
    code.replace(correct, defective, 1)
)

test_code = test.read_text()
original_import = (
    "import data_platform.spark.canonical_silver_extensions as silver"
)

replacement_import = '''import importlib.util
from pathlib import Path

module_path = Path(__file__).with_name("broken_silver.py")
spec = importlib.util.spec_from_file_location(
    "broken_silver", module_path
)
silver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(silver)'''

if test_code.count(original_import) != 1:
    raise RuntimeError("Cannot identify test module import")

(root / "test_red.py").write_text(
    test_code.replace(original_import, replacement_import, 1)
)

print("PASS isolated defective module prepared")
PY

run_spark() {
    local script="$1"
    local logfile="$2"

    docker run --rm --user root \
      -e HOME=/tmp \
      -e PYTHONPATH=/workspace \
      -v "$ROOT:/workspace:ro" \
      -v "$ROOT/infrastructure/volumes/lakehouse:/lakehouse:ro" \
      -w /workspace \
      --entrypoint /opt/spark/bin/spark-submit \
      apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu \
      --master 'local[2]' \
      --driver-memory 2g \
      --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0 \
      "$script" > "$logfile" 2>&1
}

echo "=== 2/4 RED — EXPECTED REGRESSION ==="

set +e
run_spark \
  "/workspace/$(basename "$RED_DIR")/test_red.py" \
  "$LOG_DIR/red.log"
RED_STATUS=$?
set -e

if [[ "$RED_STATUS" -eq 0 ]] ||
   ! grep -Fq \
     "AssertionError: Oracle current AP_*: wrong email" \
     "$LOG_DIR/red.log"; then
    echo "FAIL: RED did not reproduce the expected regression"
    tail -50 "$LOG_DIR/red.log"
    exit 1
fi

grep -E 'PASS Oracle|AssertionError' "$LOG_DIR/red.log"
echo "PASS RED — expected regression detected"

echo "=== 3/4 GREEN — PRODUCTION REGRESSION ==="

if ! run_spark \
  "tests/spark/test_cdc_alias_regression.py" \
  "$LOG_DIR/green.log"; then
    tail -50 "$LOG_DIR/green.log"
    exit 1
fi

if ! grep -Fq \
  "PASS: ORACLE + SQL SERVER CDC REGRESSION" \
  "$LOG_DIR/green.log"; then
    echo "FAIL: missing GREEN success marker"
    exit 1
fi

grep '^PASS ' "$LOG_DIR/green.log"
echo "PASS GREEN"

echo "=== 4/4 IDENTITY INTEGRATION ==="

if ! run_spark \
  "tests/spark/test_holder_identity_integration.py" \
  "$LOG_DIR/identity.log"; then
    tail -50 "$LOG_DIR/identity.log"
    exit 1
fi

if ! grep -Fq \
  "PASS: IDENTITY INTEGRATION — READ ONLY" \
  "$LOG_DIR/identity.log"; then
    echo "FAIL: missing identity success marker"
    exit 1
fi

grep '^PASS ' "$LOG_DIR/identity.log"

echo
echo "========================================"
echo "PASS: ALL INTEGRATION TESTS"
echo "RED:      PASS (expected failure detected)"
echo "GREEN:    PASS"
echo "IDENTITY: PASS"
echo "Logs: $LOG_DIR"
echo "========================================"
