from data_platform.spark.cdc_contract import newest_per_key

def test_latest_update_replaces_snapshot():
    events = [
        {'key': 'ORACLE:3039', 'offset': 54893, 'op': 'r', 'policy_cost': '100.0000'},
        {'key': 'ORACLE:3039', 'offset': 54986, 'op': 'u', 'policy_cost': '200.0000'},
        {'key': 'ORACLE:3039', 'offset': 54987, 'op': 'u', 'policy_cost': '300.0000'},
        {'key': 'ORACLE:3039', 'offset': 54988, 'op': 'u', 'policy_cost': '348.0000'},
    ]
    assert newest_per_key(events) == [events[-1]]

def test_delete_excludes_entity():
    assert newest_per_key([
        {'key': '1', 'offset': 1, 'op': 'c'},
        {'key': '1', 'offset': 2, 'op': 'd'},
    ]) == []

def test_independent_keys():
    assert len(newest_per_key([
        {'key': 'ORACLE:1', 'offset': 1, 'op': 'r'},
        {'key': 'SQLSERVER:1', 'offset': 1, 'op': 'r'},
    ])) == 2
