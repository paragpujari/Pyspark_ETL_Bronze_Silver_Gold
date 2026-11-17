from pyspark.sql import SparkSession
from config import SHUFFLE_PARTITIONS

def create_spark():
    return (
        SparkSession.builder
        .appName("Retail-Bronze-Silver-Gold-ETL")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
