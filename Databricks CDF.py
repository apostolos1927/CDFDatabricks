# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_table1;
# MAGIC DROP TABLE IF EXISTS silver_table;
# MAGIC DROP TABLE IF EXISTS gold_table;
# MAGIC DROP TABLE IF EXISTS gold_table1;

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

silver_data = [
    (1, 'John', 25),
    (2, 'Jane', 32),
    (3, 'Bob', 40)
]

# Create the silver table
silver_df = spark.createDataFrame(silver_data, ["id", "name", "age"])
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_table")

# Sample data for gold table
gold_data = [
    (1, 'John', 25),
    (2, 'Jane', 32)
]
# Create the gold table
gold_df = spark.createDataFrame(gold_data, ["id", "name", "age"])
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_table")

# COMMAND ----------

spark.sql("ALTER TABLE silver_table SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_table

# COMMAND ----------


# Adding new record
silver_data = [
    (5, 'Jenny', 44)
]

silver_df = spark.createDataFrame(silver_data, ["id", "name", "age"])
silver_df.write.format("delta").mode("append").saveAsTable("silver_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE silver_table
# MAGIC SET name = 'test'
# MAGIC WHERE id=2

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE silver_table
# MAGIC SET name = 'test123466611111'
# MAGIC WHERE id=2

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver_table
# MAGIC WHERE ID = 1

# COMMAND ----------

changes_df = spark.read.format("delta").option('readChangeData',True).option('startingVersion',1).table("silver_table")
changes_df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('silver_table', 1)

# COMMAND ----------

version_data = [
    (1,)
]
# Create the version table
version_df = spark.createDataFrame(version_data, ["last_version"])
version_df.write.format("delta").mode("overwrite").saveAsTable("versions_table")

# COMMAND ----------

latest_max_value = spark.read.format("delta").table("versions_table").agg({"last_version": "max"}).collect()[0][0]
print(latest_max_value)

# COMMAND ----------

spark.sql(f"SELECT * FROM (SELECT *, \
          row_number() OVER (PARTITION BY id ORDER BY _commit_timestamp desc) as RN \
          FROM table_changes ('silver_table', {latest_max_value}) \
          WHERE _change_type in ('update_postimage','insert','delete')) WHERE RN=1") \
          .createOrReplaceTempView("temp_changes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_changes

# COMMAND ----------

gold_df = spark.read.format("delta").table("gold_table")
gold_df.display()

# COMMAND ----------

# Perform the merge operation using SQL 

spark.sql("""
    MERGE INTO gold_table
    USING temp_changes
    ON gold_table.id = temp_changes.id
    WHEN MATCHED AND temp_changes._change_type= 'update_postimage' THEN
        UPDATE SET gold_table.name = temp_changes.name, gold_table.age = temp_changes.age
    WHEN MATCHED and temp_changes._change_type= 'delete' THEN
        DELETE
    WHEN NOT MATCHED and temp_changes._change_type= 'insert' THEN
        INSERT (id, name, age)
        VALUES (temp_changes.id, temp_changes.name, temp_changes.age)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  versions_table

# COMMAND ----------

latest_commit_version = spark.sql("SELECT max(_commit_version) FROM table_changes ('silver_table', 1)").collect()[0][0]
print(f"UPDATE versions_table SET last_version={latest_commit_version}")
spark.sql(f"UPDATE versions_table SET last_version={latest_commit_version}")

