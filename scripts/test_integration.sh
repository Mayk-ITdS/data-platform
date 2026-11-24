#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")/.."

ROOT="$PWD"

if [[ "${1:-}" == "--aliases-only" ]]; then

    export DEMO_WIDTH="$(tput cols 2>/dev/null || echo 120)"

    center_output() {
        python -u -c '
import os
import re
import sys

width = int(os.environ["DEMO_WIDTH"])
block_width = 100
padding = " " * max(0, (width - block_width) // 2)

for line in sys.stdin:
    print(padding + line.rstrip("\n"), flush=True)
'
    }

    exec > >(center_output) 2>&1
    export COLUMNS=100


    export PYTHONPATH="$ROOT:${PYTHONPATH:-}"
    export PYTHONUNBUFFERED=1

    BLUE='\033[1;36m'
    RED='\033[1;31m'
    GREEN='\033[1;32m'
    RESET='\033[0m'

    printf "\n${BLUE}"
    printf "==================================================\n"
    printf " FEMA DATA PLATFORM | VALIDATION BLOC 3\n"
    printf " Correction du bug : alias AB_ / AP_\n"
    printf " Fonction : typed_value()\n"
    printf "==================================================\n"
    printf "${RESET}\n"

    python - <<'PYTEST'
import tempfile
from pathlib import Path

import pytest

from data_platform.spark import canonical_silver_extensions as silver

RED = "\033[1;31m"
GREEN = "\033[1;32m"
BLUE = "\033[1;36m"
RESET = "\033[0m"

source = Path(silver.__file__)
code = source.read_text()

start = code.index(
    'raw = F.coalesce(jsonfield("record_json", "AP_" + suffix),'
)
end = code.index(
    'jsonfield("record_json", "AB_" + suffix))',
    start
) + len('jsonfield("record_json", "AB_" + suffix))')

broken_code = (
    code[:start]
    + 'raw = jsonfield("record_json", "AB_" + suffix)'
    + code[end:]
)

test_path = Path(
    "data_platform/tests/integration/test_typed_value.py"
).resolve()

labels = [
    "Resolution AB vers AP",
    "Resolution AP vers AB",
    "Priorite de l'alias AP",
    "Gestion des valeurs NULL",
    "Respect du schema source",
]


class Results:
    def __init__(self):
        self.reports = []

    def pytest_runtest_logreport(self, report):
        if report.when == "call":
            self.reports.append(report)

    def display(self):
        for label, report in zip(labels, self.reports):
            color = GREEN if report.passed else RED
            status = "PASS" if report.passed else "FAIL"
            print(f"  {label:<36} {color}{status}{RESET}")


with tempfile.TemporaryDirectory(prefix="fema-red-") as tmp:
    broken = Path(tmp) / "broken_silver.py"
    broken.write_text(broken_code)

    original = (
        "from data_platform.spark.canonical_silver_extensions"
        " import typed_value"
    )

    test_code = test_path.read_text()
    assert test_code.count(original) == 1

    red_test = Path(tmp) / "test_red.py"
    red_test.write_text(
        test_code.replace(
            original,
            "from broken_silver import typed_value",
            1
        )
    )

    print(f"\n{RED}01 | RED - AVANT CORRECTION{RESET}\n")

    results = Results()

    status = pytest.main([
        str(red_test),
        "-k", "test_typed_alias",
        "-q",
        "--tb=short",
        "--show-capture=no",
        "--disable-warnings",
        "--color=yes",
    ], plugins=[results])

    print()
    results.display()

    failures = [
        r for r in results.reports if r.failed
    ]

    if (
        status != pytest.ExitCode.TESTS_FAILED
        or len(results.reports) != 5
        or len(failures) != 2
        or not all(
            "assert result.first().age == expected" in r.longreprtext
            for r in failures
        )
    ):
        raise SystemExit(
            "ERREUR : regression attendue non reproduite"
        )

    print(f"\n{RED}Regression reproduite : "
          f"2 echecs attendus.{RESET}", flush=True)

PYTEST

    sleep 10

    printf "\n${GREEN}02 | GREEN - APRES CORRECTION${RESET}\n\n"

    python -m pytest \
        data_platform/tests/integration/test_typed_value.py \
        -k test_typed_alias \
        -q --tb=short --disable-warnings --color=yes

    printf "\n${GREEN}"
    printf "==================================================\n"
    printf " VALIDATION DE LA CORRECTION : SUCCESS\n"
    printf " RED   : regression reproduite\n"
    printf " GREEN : 5/5 tests valides\n"
    printf "==================================================\n"
    printf "${RESET}\n"

    sleep 5
    exit 0
fi

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

old = 'raw = F.coalesce(jsonfield("record_json", "AP_" + suffix),\n jsonfield("record_json", "AB_" + suffix))'
if code.count(old) != 1:
    raise RuntimeError("Cannot locate the typed AB/AP fallback for the isolated mutation")
(root / "broken_silver.py").write_text(code.replace(old, 'raw = jsonfield("record_json", "AB_" + suffix)', 1))

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
