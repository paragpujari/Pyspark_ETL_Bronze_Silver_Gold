import logging
from pyspark.sql import functions as F
from config import INPUT_DIR, OUTPUT_DIR, ENABLE_DATABRICKS_OPTIMIZE
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA, ORDER_SCHEMA
from src.ingestion import read_csv, add_bronze_metadata, write_delta
from src.transformations import (
    clean_customers, clean_products, clean_orders, add_order_reject_reason,
    split_valid_invalid, enrich_orders, build_gold
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("retail_etl")

def path(layer, table):
    return f"{OUTPUT_DIR}/{layer}/{table}"

def run_pipeline(spark):
    log.info("Starting Retail Bronze-Silver-Gold pipeline")

    # 1. INGESTION → BRONZE
    raw_customers = read_csv(spark, f"{INPUT_DIR}/customers.csv", CUSTOMER_SCHEMA)
    raw_products = read_csv(spark, f"{INPUT_DIR}/products.csv", PRODUCT_SCHEMA)
    raw_orders = read_csv(spark, f"{INPUT_DIR}/orders.csv", ORDER_SCHEMA)

    bronze_customers = add_bronze_metadata(raw_customers, "customers.csv")
    bronze_products = add_bronze_metadata(raw_products, "products.csv")
    bronze_orders = add_bronze_metadata(raw_orders, "orders.csv")

    write_delta(bronze_customers, path("bronze", "customers"))
    write_delta(bronze_products, path("bronze", "products"))
    write_delta(bronze_orders, path("bronze", "orders"))

    # 2. SILVER → clean and standardize dimensions and facts
    customers = clean_customers(bronze_customers)
    products = clean_products(bronze_products)
    orders = clean_orders(bronze_orders)

    checked_orders = add_order_reject_reason(
        orders, customers.select("customer_id"), products.select("product_id")
    )
    valid_orders, rejected_orders = split_valid_invalid(checked_orders)

    # Persist valid records because they feed both Silver and Gold.
    valid_orders = valid_orders.persist()
    rejected_orders = rejected_orders.persist()

    write_delta(customers, path("silver", "customers"))
    write_delta(products, path("silver", "products"))
    write_delta(valid_orders, path("silver", "orders"), partition_by=["order_date"])
    write_delta(rejected_orders, path("silver", "quarantine_orders"))

    # 3. GOLD → business aggregates
    enriched = enrich_orders(valid_orders, customers, products)
    # Repartition by date for parallel writes; avoid excessive partitions on small data.
    enriched = enriched.repartition("order_year", "order_month")
    daily, product, customer = build_gold(enriched)

    write_delta(daily.withColumn("order_year", F.year("order_date")),
                path("gold", "daily_sales"), partition_by=["order_year"])
    write_delta(product, path("gold", "product_performance"))
    write_delta(customer, path("gold", "customer_summary"))

    # 4. DATA QUALITY / RUN METRICS
    metrics = [
        ("bronze_orders_count", bronze_orders.count()),
        ("silver_valid_orders_count", valid_orders.count()),
        ("quarantined_orders_count", rejected_orders.count()),
        ("silver_customer_count", customers.count()),
        ("silver_product_count", products.count()),
        ("gold_daily_sales_count", daily.count()),
    ]
    metrics_df = spark.createDataFrame(metrics, ["metric_name", "metric_value"])
    write_delta(metrics_df.withColumn("run_timestamp", F.current_timestamp()),
                path("audit", "pipeline_metrics"))

    if ENABLE_DATABRICKS_OPTIMIZE:
        for table_path, zorder in [
            (path("silver", "orders"), "customer_id"),
            (path("gold", "daily_sales"), "order_date"),
        ]:
            try:
                spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder})")
                log.info("Optimized %s", table_path)
            except Exception as exc:
                log.warning("OPTIMIZE skipped for %s: %s", table_path, exc)

    valid_orders.unpersist()
    rejected_orders.unpersist()
    log.info("Pipeline completed successfully")
    return metrics_df

