"""Create real Kafka topics and deterministic Debezium-shaped test events.

No source database or Debezium connector is involved: this tests Kafka ->
production Bronze Spark -> production canonical Policy/Claim Silver.
"""
import ast
import base64
from pathlib import Path
import json
import time
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = 'kafka:9092'
TOPICS = {
    'sql_policy': 'abassurance.assurances.dbo.Policy',
    'sql_claim': 'abassurance.assurances.dbo.Claims',
    'ora_policy': 'assureplus_v2.ASSUREPLUS.POLICY',
    'ora_claim': 'assureplus_v2.ASSUREPLUS.CLAIM',
}

def number(n):
    return base64.b64encode(n.to_bytes(max(1, (n.bit_length() + 8) // 8), 'big', signed=True)).decode()

def envelope(op, after=None, before=None):
    return json.dumps({'schema': None, 'payload': {'op': op, 'before': before, 'after': after}}, separators=(',', ':')).encode()

def send(producer, topic, key, operation, after=None, before=None):
    producer.send(topic, key=json.dumps(key).encode(), value=envelope(operation, after, before)).get(timeout=30)

# Match the full subscription of the real Bronze consumer.
bronze_path = Path("data_platform/spark/bronze_ingestion.py")
tree = ast.parse(bronze_path.read_text())
topic_assignments = [
    node.value
    for node in tree.body
    if isinstance(node, ast.Assign)
    and any(
        isinstance(target, ast.Name) and target.id == "TOPICS"
        for target in node.targets
    )
]
assert len(topic_assignments) == 1
all_topics = ast.literal_eval(topic_assignments[0])
assert set(TOPICS.values()).issubset(set(all_topics))

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, request_timeout_ms=15000)
try:
    admin.create_topics([
        NewTopic(t, num_partitions=1, replication_factor=1)
        for t in all_topics
    ])
    available = set(admin.list_topics())
    assert set(all_topics).issubset(available), "Missing Kafka topics"
    print(f"PASS kafka_topics_created count={len(all_topics)}", flush=True)
finally:
    admin.close()

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all', retries=5)
try:
    # SQLServer policy 1 retained, policy 2 deleted (plus Kafka tombstone).
    send(producer, TOPICS['sql_policy'], {'PolicyId': 1}, 'c', {'PolicyId': 1, 'HolderId': 101})
    send(producer, TOPICS['sql_policy'], {'PolicyId': 2}, 'c', {'PolicyId': 2, 'HolderId': 102})
    send(producer, TOPICS['sql_policy'], {'PolicyId': 2}, 'd', before={'PolicyId': 2, 'HolderId': 102})
    producer.send(TOPICS['sql_policy'], key=json.dumps({'PolicyId': 2}).encode(), value=None).get(timeout=30)
    send(producer, TOPICS['sql_claim'], {'ClaimId': 10}, 'c', {'ClaimId': 10, 'PolicyId': 1, 'DateOfLoss': 20000})
    # Oracle NUMBER(scale=0) arrives as Debezium base64-encoded bytes.
    key = {'POLICY_RECORD_ID': number(3039)}
    for op, cost in [('c', 100), ('u', 200), ('u', 300), ('u', 348)]:
        send(producer, TOPICS['ora_policy'], key, op,
             {'POLICY_RECORD_ID': number(3039), 'HOLDER_CONTEXT': 'E2E-HOLDER', 'POLICY_COST': str(cost)})
    send(producer, TOPICS['ora_claim'], {'CLAIM_ID': number(70)}, 'c',
         {'CLAIM_ID': number(70), 'POLICY_RECORD_ID': number(3039), 'DATE_OF_LOSS': 1728000000000})
    producer.flush(timeout=30)
finally:
    producer.close()
print('PASS kafka_seeded_debezium_envelopes', flush=True)
