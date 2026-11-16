"""
Fast regression guards for the real Spark source; no JVM required.
"""
from pathlib import Path
import ast
import re

ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / "data_platform/spark/canonical_silver_extensions.py"
BRONZE = ROOT / "data_platform/spark/bronze_ingestion.py"
POLICY = ROOT / "data_platform/spark/canonical_policy_claim.py"

def test_spark_jobs_have_valid_python_syntax():
    for path in [SILVER, BRONZE, POLICY]:
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

def test_latest_raw_filters_latest_version_before_deletes():
    tree = ast.parse(SILVER.read_text(encoding="utf-8"))
    matches = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'latest_raw']
    assert len(matches) == 1
    body = ast.get_source_segment(SILVER.read_text(encoding="utf-8"), matches[0])
    assert 'row_number()' in body
    assert re.search(r'\.where\(F\.col\(["\']_rank["\']\)\s*==\s*1\)', body)
    assert body.index('== 1') < body.index('F.col("op") != "d"'), 'DELETE must be filtered AFTER latest version selection'


def test_bronze_preserves_kafka_identity_and_tombstones():
    src = BRONZE.read_text(encoding='utf-8')
    for col in ('"topic"', '"partition"', '"offset"', '"key_raw"', '"value_raw"'):
        assert col in src
    assert 'readStream' in src and 'writeStream' in src
    assert 'value_raw").isNotNull()' not in src, 'Bronze must preserve Kafka tombstones'


def test_source_keeps_policy_and_claim_separate():
    src = POLICY.read_text(encoding='utf-8')
    assert 'write_table(policy, "policy")' in src
    assert 'write_table(claim, "claim")' in src
