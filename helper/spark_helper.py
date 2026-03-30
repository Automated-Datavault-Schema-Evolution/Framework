import os

from logger import log
from pyspark.sql import SparkSession

from config.config import (
    ALLOW_MAVEN,
    DRIVER_PY,
    EXEC_PY,
    SPARK_ADAPTIVE_EXECUTION,
    SPARK_DRIVER_CORES,
    SPARK_DRIVER_MEMORY,
    SPARK_DYNAMIC_ALLOCATION,
    SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS,
    SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS,
    SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS,
    SPARK_DYNAMIC_SHUFFLE_TRACKING,
    SPARK_EXECUTOR_CORES,
    SPARK_EXECUTOR_MEMORY,
    SPARK_IVY_PATH,
    SPARK_KRYO_BUFFER_MAX,
    SPARK_MASTER,
    SPARK_SERIALIZER,
    SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE,
    SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS,
    SPARK_SQL_SHUFFLE_PARTITIONS,
    SPARK_UI_PORT,
)


def get_spark_session(app_name: str = "Kafka_Consumer_Lake_Handler"):
    log.info(f"Initializing Spark session '{app_name}' on master '{SPARK_MASTER}'")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .enableHiveSupport()
        .config("spark.pyspark.driver.python", DRIVER_PY)
        .config("spark.pyspark.python", EXEC_PY)
        .config("spark.executorEnv.PYSPARK_PYTHON", EXEC_PY)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.ivy", SPARK_IVY_PATH)
        .config("spark.driver.extraJavaOptions", "-Duser.home=/tmp")
        .config("spark.executor.extraJavaOptions", "-Duser.home=/tmp")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.cores", SPARK_DRIVER_CORES)
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.serializer", SPARK_SERIALIZER)
        .config("spark.kryoserializer.buffer.max", SPARK_KRYO_BUFFER_MAX)
        .config("spark.sql.adaptive.enabled", str(SPARK_ADAPTIVE_EXECUTION).lower())
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            str(SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS).lower(),
        )
        .config(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes",
            SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE,
        )
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .config("spark.sql.streaming.kafka.useUninterruptibleThread", "true")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", SPARK_UI_PORT)
        .config("spark.ui.reverseProxy", "true")
        .config("spark.ui.proxyBase", "/spark/dv/app")
    )

    if SPARK_DYNAMIC_ALLOCATION:
        builder = (
            builder.config("spark.dynamicAllocation.enabled", "true")
            .config(
                "spark.dynamicAllocation.shuffleTracking.enabled",
                str(SPARK_DYNAMIC_SHUFFLE_TRACKING).lower(),
            )
            .config("spark.dynamicAllocation.minExecutors", SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS)
            .config("spark.dynamicAllocation.maxExecutors", SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS)
            .config("spark.dynamicAllocation.initialExecutors", SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS)
        )

    requested_jars = [
        "/opt/ext-jars/spark-sql-kafka-0-10_2.12-3.5.6.jar",
        "/opt/ext-jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar",
        "/opt/ext-jars/kafka-clients-3.5.1.jar",
        "/opt/jdbc-jars/postgresql-42.7.4.jar",
        "/opt/ext-jars/commons-pool2-2.12.1.jar",
    ]
    existing_jars = [path for path in requested_jars if os.path.exists(path)]
    for path in requested_jars:
        log.info(f"[SEF_HELPER][JAR_CHECK] {path} exists={os.path.exists(path)}")

    if existing_jars:
        builder = builder.config("spark.jars", ",".join(existing_jars))
    else:
        log.warning(
            "[SEF_HELPER][JAR_CHECK] None of the expected jars were found under "
            "/opt/(delta-jars|ext-jars). Spark may try Ivy if ALLOW_MAVEN is enabled."
        )

    if ALLOW_MAVEN:
        builder = builder.config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
                    "org.apache.kafka:kafka-clients:3.5.1",
                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6",
                    "org.postgresql:postgresql:42.7.4",
                    "org.apache.commons:commons:2.12.1",
                ]
            ),
        )

    spark = builder.getOrCreate()
    log.debug(f"Spark configuration: {spark.sparkContext.getConf().getAll()}")
    return spark
