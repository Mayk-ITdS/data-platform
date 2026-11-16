"""Pure CDC decision contract. To reuse in production, integrate with Spark reducer."""
from typing import Any

def newest_per_key(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Snapshot from one topic: latest offset for each key; skip deleted keys.

    Production caveat: for multi-partition topics compare an ordering token
    only when events are for the same key and Kafka partition.
    """
    last: dict[str, dict[str, Any]] = {}
    for e in events:
        key = str(e['key'])
        old = last.get(key)
        if old is None or int(e['offset']) > int(old['offset']):
            last[key] = e
    return [e for e in last.values() if e['op'] != 'd']
