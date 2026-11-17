from pyspark.sql.functions import current_timestamp, input_file_name, lit

def read_csv(spark, path, schema):
    return (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )

def add_bronze_metadata(df, source_name):
    return (
        df.withColumn("_source_file", input_file_name())
          .withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_name", lit(source_name))
    )

def write_delta(df, path, mode="overwrite", partition_by=None):
    writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
