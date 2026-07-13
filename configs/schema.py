from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field, field_validator

class PipelineType(str, Enum):
    REALTIME_INTEGRATION = "realtime_integration"
    BATCH_TRANSFORM = "batch_transform"
    DBT_TRANSFORM = "dbt_transform"
    EXPORT = "export"
    QUALITY_CHECK = "quality_check"
    MAINTENANCE = "maintenance"

class SourceType(str, Enum):
    SQLSERVER = "sqlserver"
    ORACLE = "oracle"
    POSTGRES = "postgres"
    LAKEHOUSE_TABLE = "lakehouse_table"
    FILE = "file"
    API = "api"

class CdcConfig(BaseModel):
    engine: str
    snapshot_mode: str = "initial"

class SourceConfig(BaseModel):
    name: str
    type: SourceType
    connection_secret_ref: str
    tables: List[str]
    cdc: Optional[CdcConfig] = None

    @field_validator("connection_secret_ref")
    @classmethod
    def secret_ref_must_not_be_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("connection_secret_ref cannot be empty")
        return value

class KafkaConfig(BaseModel):
    topic_prefix: str
    partitions: int = Field(gt=0)
    replication_factor: int = Field(gt=0)
    retention_days: int = Field(gt=0)
    dlq_topic: str


class SparkResources(BaseModel):
    driver_memory: str
    executor_memory: str
    executor_instances: int = Field(gt=0)


class ProcessingConfig(BaseModel):
    engine: str
    transformation_module: str
    checkpoint_location: str
    resources: SparkResources


class StorageConfig(BaseModel):
    profile: str
    bronze_namespace: str
    silver_namespace: str
    gold_namespace: str


class DbtConfig(BaseModel):
    enabled: bool = False
    project: Optional[str] = None
    selector: Optional[str] = None


class ServingTarget(BaseModel):
    type: str
    use_case: Optional[str] = None
    source_table: Optional[str] = None

class PipelineConfig(BaseModel):
    pipeline_id: str
    domain: str
    type: PipelineType
    sources: List[SourceConfig]
    kafka: KafkaConfig
    processing: ProcessingConfig
    storage: StorageConfig
    dbt: Optional[DbtConfig] = None
    serving: List[ServingTarget] = []

    @field_validator("sources")
    @classmethod
    def realtime_integration_requires_sources(cls, value: List[SourceConfig]) -> List[SourceConfig]:
        if not value:
            raise ValueError("At least one source is required")
        return value