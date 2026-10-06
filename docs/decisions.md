# Architectural decisions

- Kubernetes is self-managed with kubeadm and containerd.
- Storage foundation: Rook-Ceph.
- Ceph RBD/CephFS: Kubernetes persistent storage.
- Ceph RGW: S3-compatible object interface for Iceberg.
- Kafka is managed by Strimzi.
- CDC is implemented through Kafka Connect and Debezium.
- Stream processing uses Spark Structured Streaming.
- Batch processing uses Spark.
- Table format: Apache Iceberg.
- Transformations: dbt.
- Query layer: Trino.
- Orchestration: Airflow.
- The DataPipeline operator is implemented first with the native Kubernetes Python client.
- Kopf must not be used during Stage 1.
- The platform must support both CDC and massive batch ingestion.