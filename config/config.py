import os
from typing import Tuple

from dotenv import load_dotenv


_TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def _get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in _TRUE_VALUES


def _get_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _get_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _get_csv_tuple(name: str, default: str) -> Tuple[str, ...]:
    value = os.getenv(name, default)
    return tuple(item.strip() for item in value.split(",") if item.strip())


env_type = os.getenv("ENV_TYPE", "local")
load_dotenv(f".env.{env_type}")

DEFAULT_POLICY_NAME = os.getenv("SEF_DEFAULT_POLICY", "production")
SEF_POLICY_CONFIG = os.getenv("SEF_POLICY_CONFIG")

LAKE_TYPE = os.getenv("LAKE_TYPE", "rdbms")
PARQUET_PATH = os.getenv("PARQUET_PATH", "/opt/parquet_files")

SEF_CHECKPOINT_DIR = os.environ.get("SEF_CHECKPOINT_DIR", "/opt/sef/_chk")
TECHNICAL_COLUMNS = _get_csv_tuple("SEF_TECHNICAL_COLUMNS", "ingestion_timestamp")

DRIVER_PY = os.getenv("DRIVER_PY", "/usr/local/bin/python")
EXEC_PY = os.getenv("EXEC_PY", "/opt/bitnami/python/bin/python")
SPARK_IVY_PATH = os.getenv("SPARK_IVY_PATH", "/tmp/.ivy2")
ALLOW_MAVEN = _get_bool("ALLOW_MAVEN", default=False)

RDBMS_HOST = os.getenv("POSTGRES_HOST", "localhost")
RDBMS_PORT = _get_int("POSTGRES_PORT", 5432)
RDBMS_DB = os.getenv("POSTGRES_DB")
RDBMS_USER = os.getenv("POSTGRES_USER")
RDBMS_PASSWORD = os.getenv("POSTGRES_PASSWORD")
RDBMS_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_NOTIFICATIONS = os.getenv("KAFKA_TOPIC_NOTIFICATION", "filewatcher.events")
KAFKA_TOPIC_SCHEMA_EVOLVED = os.getenv("KAFKA_TOPIC_SCHEMA_EVOLVED", "schema.evolved")
KAFKA_TOPIC_SCHEMA_FAILED = os.getenv("KAFKA_TOPIC_SCHEMA_FAILED", "schema.evolution.failed")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sef_core")
KAFKA_PARTITIONS = _get_int("KAFKA_PARTITIONS", 8)
KAFKA_REPLICATION = _get_int("KAFKA_REPLICATION", 1)
KAFKA_MAX_OFFSETS_PER_TRIGGER = _get_int("KAFKA_MAX_OFFSETS_PER_TRIGGER", 50000)

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://sef-spark-master:7078")
SPARK_UI_PORT = os.getenv("SPARK_UI_PORT", "4053")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "6g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "6g")
SPARK_DRIVER_CORES = os.getenv("SPARK_DRIVER_CORES", "4")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "4")
SPARK_SQL_SHUFFLE_PARTITIONS = _get_int("SPARK_SQL_SHUFFLE_PARTITIONS", 200)
SPARK_DYNAMIC_ALLOCATION = _get_bool("SPARK_DYNAMIC_ALLOCATION", default=False)
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = _get_int("SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS", 1)
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = _get_int("SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS", 10)
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = _get_int("SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS", 1)
SPARK_SERIALIZER = os.getenv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer")
SPARK_KRYO_BUFFER_MAX = os.getenv("SPARK_KRYO_BUFFER_MAX", "256m")
SPARK_ADAPTIVE_EXECUTION = _get_bool("SPARK_ADAPTIVE_EXECUTION", default=True)
SPARK_DYNAMIC_SHUFFLE_TRACKING = _get_bool("SPARK_DYNAMIC_SHUFFLE_TRACKING", default=True)
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS = _get_bool("SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS", default=True)
SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE = os.getenv("SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE", "64m")

THRIFT_HOST = os.getenv("THRIFT_HOST", "localhost")
THRIFT_PORT = _get_int("THRIFT_PORT", 10000)
THRIFT_AUTH = os.getenv("THRIFT_AUTH", "NOSASL")

METADATA_LINEAGE_TYPE = os.getenv("METADATA_LINEAGE_TYPE", "parquet")
METADATA_LINEAGE_PATH = os.getenv("METADATA_LINEAGE_PATH", "meta/lineage.parquet")
METADATA_METADATA_PATH = os.getenv("METADATA_METADATA_PATH", "meta/metadata.parquet")
METASTORE_DB_HOST = os.getenv("METASTORE_DB_HOST", RDBMS_HOST)
METASTORE_DB_PORT = _get_int("METASTORE_DB_PORT", RDBMS_PORT)
METASTORE_DB = os.getenv("METASTORE_DB", "META_MART")
METASTORE_DB_USER = os.getenv("METASTORE_DB_USER", RDBMS_USER)
METASTORE_DB_PASSWORD = os.getenv("METASTORE_DB_PASSWORD", RDBMS_PASSWORD)
METASTORE_DB_SCHEMA = os.getenv("METASTORE_DB_SCHEMA", "metastore")
METASTORE_URI = os.getenv("METASTORE_URI", "thrift://hive-metastore:9083")
SEF_METADATA_STORE_RAW = os.getenv("SEF_METADATA_STORE", "")
SEF_METASTORE_SCHEMA = os.getenv("SEF_METASTORE_SCHEMA", "metastore")
METASTORE_POOL_MIN = _get_int("METASTORE_POOL_MIN", 1)
METASTORE_POOL_MAX = _get_int("METASTORE_POOL_MAX", 5)

STREAM_TRIGGER = os.getenv("STREAM_TRIGGER", "2 seconds")
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "streaming")

LAKE_HANDLER_GRPC_TARGET = os.getenv("LAKE_HANDLER_GRPC_TARGET", "DataLakeIngestionHandler:50051")
VAULT_HANDLER_GRPC_TARGET = os.getenv("VAULT_HANDLER_GRPC_TARGET", "DatavaultIngestionHandler:50052")
GRPC_TIMEOUT_S = _get_int("GRPC_TIMEOUT_S", 200)
SEF_VERIFY_MAX_WAIT_S = _get_float("SEF_VERIFY_MAX_WAIT_S", 120.0)
SEF_VERIFY_INTERVAL_S = _get_float("SEF_VERIFY_INTERVAL_S", 1.0)

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dbt")

MAX_RETRIES = _get_int("SEF_EXECUTOR_MAX_RETRIES", 20)
BACKOFF_INITIAL_S = _get_float("SEF_EXECUTOR_BACKOFF_INITIAL_S", 0.25)
BACKOFF_MULT = _get_float("SEF_EXECUTOR_BACKOFF_MULT", 2.0)
BACKOFF_MAX_S = _get_float("SEF_EXECUTOR_BACKOFF_MAX_S", 3.0)

SEF_DEBUG_KAFKA_PAYLOAD = _get_bool("SEF_DEBUG_KAFKA_PAYLOAD", default=False)
SEF_DEBUG_TRACE = _get_bool("SEF_DEBUG_TRACE", default=False)
SEF_DEBUG_KAFKA_PAYLOAD_MAX_CHARS = _get_int("SEF_DEBUG_KAFKA_PAYLOAD_MAX_CHARS", 20000)
