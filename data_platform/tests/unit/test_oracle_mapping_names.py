import pytest

from data.source_system.dbs.oracle.migrations.oracle_mappings import to_snake_case


@pytest.mark.parametrize(("name", "expected"), [
    ("", ""), ("XMLParser", "xml_parser"), ("ZIPCode2Value", "zip_code2_value"),
    ("PolicyId", "policy_id"), ("already_snake", "already_snake"), ("ABC", "abc"),
])
def test_oracle_mapping_boundaries(name, expected):
    assert to_snake_case(name) == expected
