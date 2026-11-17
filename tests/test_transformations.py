from pyspark.sql import SparkSession
from src.transformations import clean_customers, clean_products, clean_orders

def test_customer_standardization_and_deduplication():
    spark = SparkSession.builder.master("local[1]").appName("unit-test").getOrCreate()
    try:
        df = spark.createDataFrame([
            (" c001 ", "aarav sharma", "hyderabad", "telangana", " A@EXAMPLE.COM "),
            ("C001", "Aarav Sharma", "Hyderabad", "Telangana", "a@example.com"),
        ], ["customer_id", "customer_name", "city", "state", "email"])
        result = clean_customers(df)
        assert result.count() == 1
        row = result.first()
        assert row.customer_id == "C001"
        assert row.email == "a@example.com"
    finally:
        spark.stop()
