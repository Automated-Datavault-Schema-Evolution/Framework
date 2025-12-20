import os

from dotenv import load_dotenv

env_type = os.getenv('ENV_TYPE', "local")
load_dotenv(f".env.{env_type}")

LAKE_TYPE = os.getenv("LAKE_TYPE", "rdbms")
PARQUET_PATH = os.getenv("PARQUET_PATH", "/opt/parquet_files")

# SEF internals
SEF_CHECKPOINT_DIR = os.environ.get("SEF_CHECKPOINT_DIR", "/opt/sef/_chk")

# Python executor paths
DRIVER_PY = os.getenv("DRIVER_PY", "/usr/local/bin/python")
EXEC_PY = os.getenv("EXEC_PY", "/opt/bitnami/python/bin/python")
SPARK_IVY_PATH = os.getenv("SPARK_IVY_PATH", "/tmp/.ivy2")

# RDBMS settings
RDBMS_HOST = os.getenv("POSTGRES_HOST", "localhost")
RDBMS_PORT = int(os.getenv("POSTGRES_PORT", 5432))
RDBMS_DB = os.getenv("POSTGRES_DB")
RDBMS_USER = os.getenv("POSTGRES_USER")
RDBMS_PASSWORD = os.getenv("POSTGRES_PASSWORD")
RDBMS_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_NOTIFICATIONS = os.getenv("KAFKA_TOPIC", "filewatcher.events")
KAFKA_TOPIC_SCHEMA_EVOLVED = os.getenv("KAFKA_TOPIC", "schema.evolved")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sef_core")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "8"))
KAFKA_REPLICATION = int(os.getenv("KAFKA_REPLICATION", "1"))
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50000"))

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://sef-spark-master:7078")
SPARK_UI_PORT = os.getenv("SPARK_UI_PORT", "4053")
# Spark resource tuning parameters
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "6g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "6g")
SPARK_DRIVER_CORES = os.getenv("SPARK_DRIVER_CORES", "4")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "4")
SPARK_SQL_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
SPARK_DYNAMIC_ALLOCATION = os.getenv("SPARK_DYNAMIC_ALLOCATION", "false").lower() == "true"
# Optional dynamic allocation parameters
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS", 1))
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS", 10))
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS", 1))
# Additional Spark tuning
SPARK_SERIALIZER = os.getenv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer")
SPARK_KRYO_BUFFER_MAX = os.getenv("SPARK_KRYO_BUFFER_MAX", "256m")
SPARK_ADAPTIVE_EXECUTION = os.getenv("SPARK_ADAPTIVE_EXECUTION", "true").lower() == "true"
SPARK_DYNAMIC_SHUFFLE_TRACKING = os.getenv("SPARK_DYNAMIC_SHUFFLE_TRACKING", "true").lower() == "true"
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS = os.getenv("SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS", "true").lower() == "true"
SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE = os.getenv("SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE", "64m")

# Thift Connection
THRIFT_HOST = os.getenv("THRIFT_HOST", "localhost")
THRIFT_PORT = int(os.getenv("THRIFT_PORT", "10000"))
THRIFT_AUTH = os.getenv("THRIFT_AUTH", "NOSASL")

# Metadata / lineage repository
METADATA_LINEAGE_TYPE = os.getenv("METADATA_LINEAGE_TYPE", "parquet")
METADATA_LINEAGE_PATH = os.getenv("METADATA_LINEAGE_PATH", "meta/lineage.parquet")
METADATA_METADATA_PATH = os.getenv("METADATA_METADATA_PATH", "meta/metadata.parquet")
METASTORE_DB_HOST = os.getenv("METASTORE_DB_HOST", RDBMS_HOST)
METASTORE_DB_PORT = int(os.getenv("METASTORE_DB_PORT", RDBMS_PORT))
METASTORE_DB = os.getenv("METASTORE_DB", 'META_MART')
METASTORE_DB_USER = os.getenv("METASTORE_DB_USER", RDBMS_USER)
METASTORE_DB_PASSWORD = os.getenv("METASTORE_DB_PASSWORD", RDBMS_PASSWORD)
METASTORE_DB_SCHEMA = os.getenv("METASTORE_DB_SCHEMA", 'metastore')
METASTORE_URI = os.getenv("METASTORE_URI", "thrift://hive-metastore:9083")

# Default stream trigger; keep your existing env var if you have one.
STREAM_TRIGGER = os.getenv("STREAM_TRIGGER", "2 seconds")
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "streaming")

# Handler gRPC endpoints (TODO: change to default docker network names and ports)
LAKE_HANDLER_GRPC_TARGET = os.getenv("LAKE_HANDLER_GRPC_TARGET", "DataLakeIngestionHandler:50051")
VAULT_HANDLER_GRPC_TARGET = os.getenv("VAULT_HANDLER_GRPC_TARGET", "DatavaultIngestionHandler:50052")

# DBT
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/dbt")
