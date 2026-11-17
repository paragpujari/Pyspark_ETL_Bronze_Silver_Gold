from pyspark.sql import functions as F
from pyspark.sql.window import Window

def clean_customers(df):
    return (
        df.withColumn("customer_id", F.upper(F.trim("customer_id")))
          .withColumn("customer_name", F.initcap(F.trim("customer_name")))
          .withColumn("city", F.initcap(F.trim("city")))
          .withColumn("state", F.initcap(F.trim("state")))
          .withColumn("email", F.lower(F.trim("email")))
          .filter(F.col("customer_id").isNotNull())
          .dropDuplicates(["customer_id"])
    )

def clean_products(df):
    return (
        df.withColumn("product_id", F.upper(F.trim("product_id")))
          .withColumn("product_name", F.trim("product_name"))
          .withColumn("category", F.initcap(F.trim("category")))
          .withColumn("list_price", F.col("list_price").cast("double"))
          .filter(F.col("product_id").isNotNull())
          .filter(F.col("list_price").isNotNull() & (F.col("list_price") >= 0))
          .dropDuplicates(["product_id"])
    )

def clean_orders(df):
    # Keep raw date text until parsing so invalid dates can be quarantined.
    parsed = (
        df.withColumn("order_id", F.upper(F.trim("order_id")))
          .withColumn("customer_id", F.upper(F.trim("customer_id")))
          .withColumn("product_id", F.upper(F.trim("product_id")))
          .withColumn("status", F.initcap(F.trim("status")))
          .withColumn("quantity", F.col("quantity").cast("int"))
          .withColumn("unit_price", F.col("unit_price").cast("double"))
          .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    )
    # Deterministic deduplication: one row per order_id; all input fields are identical
    # in this demo. In production, use a source update timestamp in the window ordering.
    return parsed.dropDuplicates(["order_id"])

def add_order_reject_reason(orders, customer_ids, product_ids):
    customers = customer_ids.select(F.col("customer_id").alias("_valid_customer_id"))
    products = product_ids.select(F.col("product_id").alias("_valid_product_id"))
    checked = (
        orders.join(F.broadcast(customers),
                    orders.customer_id == F.col("_valid_customer_id"), "left")
              .join(F.broadcast(products),
                    orders.product_id == F.col("_valid_product_id"), "left")
    )
    reason = F.concat_ws(
        "; ",
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "missing order_id"),
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "missing customer_id"),
        F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "missing product_id"),
        F.when(F.col("order_date").isNull(), "invalid or missing order_date"),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "quantity must be greater than zero"),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "unit_price must be non-negative"),
        F.when(F.col("_valid_customer_id").isNull(), "customer_id not found in customer master"),
        F.when(F.col("_valid_product_id").isNull(), "product_id not found in product master"),
    )
    return checked.withColumn("reject_reason", reason).drop("_valid_customer_id", "_valid_product_id")

def split_valid_invalid(checked):
    valid = checked.filter(F.col("reject_reason") == "").drop("reject_reason")
    invalid = checked.filter(F.col("reject_reason") != "")
    return valid, invalid

def enrich_orders(valid_orders, customers, products):
    return (
        valid_orders.join(customers, "customer_id", "inner")
                    .join(products, "product_id", "inner")
                    .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
                    .withColumn("order_year", F.year("order_date"))
                    .withColumn("order_month", F.month("order_date"))
    )

def build_gold(enriched):
    daily = (
        enriched.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total"), 2).alias("total_revenue"),
        )
    )
    product = (
        enriched.groupBy("product_id", "product_name", "category")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total"), 2).alias("total_revenue"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    customer = (
        enriched.groupBy("customer_id", "customer_name", "city", "state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("line_total"), 2).alias("lifetime_spend"),
            F.max("order_date").alias("last_order_date"),
        )
    )
    return daily, product, customer
