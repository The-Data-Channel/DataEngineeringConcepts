# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/source_data.csv

# COMMAND ----------

def refresh_datasets():
    data_source = [
        (1, "Alice", "alice@example.com", "2024-12-30"),
        (2, "Bob", "bob@example.com", "2024-12-30"),
        (3, "Carol", "carol_new@example.com", "2025-01-05"),
        (4, "David", "david@example.com", "2025-01-06"),
        (5, "Eve", "eve@example.com", "2025-01-06"),
        (6, "Frank", "frank@example.com", "2025-01-07"),
    ]

    data_target = [
        (1, "Alice", "alice@example.com", "2024-12-30"),
        (2, "Bob", "bob@example.com", "2024-12-30"),
        (3, "Carol", "carol@example.com", "2024-12-25"),
        (4, "David", "david@example.com", "2025-01-01"),
    ]

    snapshot_yesterday = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Carol", "carol@example.com"),
        (4, "David", "david@example.com"),
    ]

    snapshot_today = [
        (1, "Alice", "alice@example.com"),
        (3, "Carol", "carol_new@example.com"),
        (4, "David", "david@example.com"),
        (5, "Eve", "eve@example.com"),
    ]

    schema_with_update = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("last_updated", StringType(), True),
    ])

    schema_no_update = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ])

    spark.createDataFrame(data_source, schema_with_update)\
        .write.mode("overwrite").option("header", True).csv("/FileStore/tables/source_data.csv")

    spark.createDataFrame(data_target, schema_with_update)\
        .write.mode("overwrite").option("header", True).csv("/FileStore/tables/target_data.csv")

    spark.createDataFrame(snapshot_yesterday, schema_no_update)\
        .write.mode("overwrite").option("header", True).csv("/FileStore/tables/source_snapshot_yesterday.csv")

    spark.createDataFrame(snapshot_today, schema_no_update)\
        .write.mode("overwrite").option("header", True).csv("/FileStore/tables/source_snapshot_today.csv")



# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #Common Data loading models 
# MAGIC ##1. Full Load – Truncate & Reload
# MAGIC ##2. Incremental Load – Append or Upsert
# MAGIC ##3. Differential Load – Snapshot Comparison
# MAGIC ##4. Change Data Capture (CDC) – Log or Trigger Based

# COMMAND ----------

# MAGIC %md
# MAGIC ##Full Load – Truncate & Reload
# MAGIC ###Definition:
# MAGIC In this approach, the target table is completely emptied (usually via TRUNCATE) and then fully reloaded with the current source data.
# MAGIC
# MAGIC ##How It Works:
# MAGIC Delete all records in the data warehouse table.
# MAGIC
# MAGIC Read the full dataset from the source.
# MAGIC
# MAGIC Load it into the warehouse table from scratch.
# MAGIC
# MAGIC ##When to Use:
# MAGIC Data volume is relatively small.
# MAGIC
# MAGIC No reliable method exists to detect changes.
# MAGIC
# MAGIC Business requires full refresh due to compliance or data integrity.
# MAGIC
# MAGIC ##Pros:
# MAGIC Simplicity: Easy to implement.
# MAGIC
# MAGIC Consistency: Guarantees target is always in sync with source.
# MAGIC
# MAGIC ##Cons:
# MAGIC Inefficient for large datasets: Wastes resources by reloading unchanged data.
# MAGIC
# MAGIC Downtime: Table is unavailable during reload.
# MAGIC
# MAGIC Heavy I/O and compute usage.
# MAGIC
# MAGIC

# COMMAND ----------

refresh_datasets()

# Read source data
source_df = spark.read.option("header", True).csv("/FileStore/tables/source_data.csv")
source_df.show()

# TRUNCATE target and reload
source_df.write.mode("overwrite").option("header", True).csv("/FileStore/tables/target_data.csv")

# Verify
spark.read.option("header", True).csv("/FileStore/tables/target_data.csv").show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Incremental Load – Append or Upsert
# MAGIC ##Definition:
# MAGIC Only the new or modified records since the last load are processed and written to the warehouse.
# MAGIC
# MAGIC ##Types:
# MAGIC Append: Only new records are added.
# MAGIC
# MAGIC Upsert: Existing records are updated, and new ones are inserted.
# MAGIC
# MAGIC ##How It Works:
# MAGIC Track changes using mechanisms like:
# MAGIC
# MAGIC last_updated timestamp.
# MAGIC
# MAGIC Surrogate keys (monotonically increasing IDs).
# MAGIC
# MAGIC Audit columns in source system.
# MAGIC
# MAGIC ##When to Use:
# MAGIC Large datasets with few changes per load.
# MAGIC
# MAGIC High frequency data refresh required.
# MAGIC
# MAGIC ##Pros:
# MAGIC Efficient and fast: Only processes deltas.
# MAGIC
# MAGIC Scalable: Supports large, growing datasets.
# MAGIC
# MAGIC ##Cons:
# MAGIC Change detection must be accurate.
# MAGIC
# MAGIC Complex logic for deduplication and conflict resolution.

# COMMAND ----------

from pyspark.sql.functions import col, to_date

refresh_datasets()

# Read both
source_df = spark.read.option("header", True).csv("/FileStore/tables/source_data.csv")
target_df = spark.read.option("header", True).csv("/FileStore/tables/target_data.csv")

# Convert to date
source_df = source_df.withColumn("last_updated", to_date("last_updated"))
target_df = target_df.withColumn("last_updated", to_date("last_updated"))

source_df.show()
target_df.show()

# Simulate last run time
from pyspark.sql.functions import lit
last_run = "2024-12-31" # Meta data- cofig DB/ Filesystem
incremental_df = source_df.filter(col("last_updated") > lit(last_run))
#incremental_df.show()

# UPSERT logic
# Remove rows in target that exist in incremental
target_dedup = target_df.join(incremental_df.select("id"), on="id", how="left_anti")
#target_dedup.show()

# Union
merged_df = target_dedup.unionByName(incremental_df)
#merged_df.show()

# Save back
merged_df.write.mode("overwrite").option("header", True).csv("/FileStore/tables/target_data.csv")

target_df_final = spark.read.option("header", True).csv("/FileStore/tables/target_data.csv")
target_df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Differential Load – Delta Based on Snapshot Comparison
# MAGIC ##Definition:
# MAGIC Compares current source snapshot with a previously stored snapshot to detect inserts, updates, and deletes.
# MAGIC
# MAGIC ##How It Works:
# MAGIC Extract a full snapshot from the source system.
# MAGIC
# MAGIC Store it and compare it row-by-row with the previous snapshot.
# MAGIC
# MAGIC ##Identify:
# MAGIC
# MAGIC Inserts: Rows in current snapshot but not in the previous one.
# MAGIC
# MAGIC Updates: Rows with matching keys but different values.
# MAGIC
# MAGIC Deletes: Rows in previous snapshot but not in the current one.
# MAGIC
# MAGIC ##When to Use:
# MAGIC No last_updated or CDC mechanisms available.
# MAGIC
# MAGIC Source system doesn't support incremental logic.
# MAGIC
# MAGIC ##Pros:
# MAGIC Independent of source system features.
# MAGIC
# MAGIC Flexible: Works even with legacy systems.
# MAGIC
# MAGIC ##Cons:
# MAGIC High compute due to comparison logic.
# MAGIC
# MAGIC Additional storage needed for snapshots.
# MAGIC
# MAGIC Latency: Cannot support real-time loads.

# COMMAND ----------

refresh_datasets()

yesterday = spark.read.option("header", True).csv("/FileStore/tables/source_snapshot_yesterday.csv")
today = spark.read.option("header", True).csv("/FileStore/tables/source_snapshot_today.csv")
yesterday.show()
today.show()

# Inserts: in today but not yesterday
inserts = yesterday.join(today, on="id", how="left_anti")
print("🔼 Inserts")
inserts.show()

# Deletes: in yesterday but not today
deletes = today.join(yesterday, on="id", how="left_anti")
print("🔽 Deletes")
deletes.show()

# Updates: id exists in both, but values differ
updates = today.alias("t").join(
    yesterday.alias("y"), on="id"
).filter("t.name != y.name OR t.email != y.email")
print("🔁 Updates")
updates.select("t.*").show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Change Data Capture (CDC) – Log-based or Trigger-based
# MAGIC ##Definition:
# MAGIC CDC captures row-level changes (INSERT, UPDATE, DELETE) from database logs or triggers, and replicates them to the target.
# MAGIC
# MAGIC ##Types:
# MAGIC Log-based CDC: Reads changes from database transaction logs (e.g., MySQL binlog, SQL Server CDC, Oracle redo logs).
# MAGIC
# MAGIC Trigger-based CDC: Uses database triggers to capture changes and write them to tracking tables.
# MAGIC
# MAGIC ##Example
# MAGIC Structured Streaming
# MAGIC
# MAGIC Delta Lake (with Delta Change Data Feed or MERGE operations)
# MAGIC
# MAGIC Auto Loader
# MAGIC
# MAGIC Streaming from message queues like Kafka
# MAGIC
# MAGIC ##How It Works:
# MAGIC The CDC process continuously reads the change log.
# MAGIC
# MAGIC Translates changes into actionable operations.
# MAGIC
# MAGIC Applies changes to the data warehouse with minimal latency.
# MAGIC
# MAGIC ##When to Use:
# MAGIC Real-time or near real-time use cases.
# MAGIC
# MAGIC Modern data pipelines (streaming ETL/ELT).
# MAGIC
# MAGIC Systems with high data change velocity.
# MAGIC
# MAGIC ##Pros:
# MAGIC Real-time updates: Keeps target highly synchronized.
# MAGIC
# MAGIC Accurate: Captures precise changes.
# MAGIC
# MAGIC ##Cons:
# MAGIC Complex setup: Requires deep integration with source DB.
# MAGIC
# MAGIC Permissions and performance: May require elevated privileges and impact source DB performance.
# MAGIC
# MAGIC Not universally supported: Some databases don't expose log access.

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/schema_location')

# COMMAND ----------

from pyspark.sql.functions import *

# Define the source path (without /dbfs prefix)
source_path = "/FileStore/tables/source_location"  # This is relative to /dbfs

# Define the target Delta table path
target_path = "/FileStore/tables/target_location"

schema_path="/FileStore/tables/schema_location"

# Start the Auto Loader stream
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

# Optional transformation
df_transformed = df.withColumn("ingest_time", current_timestamp())

# Write to Delta table in append mode
(
    df_transformed.writeStream
    .format("csv")
    .option("checkpointLocation", "/FileStore/tables/checkpoint_location")
    .outputMode("append")
    .start(target_path)
)


# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/target_location

# COMMAND ----------

df=spark.read.csv("/FileStore/tables/target_location")
display(df)

# COMMAND ----------

