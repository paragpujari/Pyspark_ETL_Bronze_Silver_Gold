# Project 3: End-to-End PySpark ETL Pipeline (Bronze → Silver → Gold)

A portfolio-ready batch ETL project using **PySpark + Delta Lake**. It ingests retail CSV files, preserves raw data in Bronze, cleans and validates records in Silver, and builds business-ready Gold tables.

## Architecture

```text
data/customers.csv ─┐
data/products.csv  ─┼─> Bronze Delta (raw + ingestion metadata)
data/orders.csv    ─┘                 |
                                      v
                         Silver Delta (typed, cleaned,
                         deduplicated, validated)
                           | valid       | rejected
                           v             v
                    Silver tables   quarantine Delta
                           |
                           v
                    Gold Delta tables
                    - daily_sales
                    - product_performance
                    - customer_summary
```

## What this project demonstrates

- CSV ingestion with explicit schemas and corrupt/malformed row handling
- Bronze/Silver/Gold medallion architecture
- Type casting, trimming, standardization, null handling and deduplication
- Data quality checks, rejected-record quarantine and audit metrics
- Inner/left joins and aggregations
- Date-based partitioning and controlled repartitioning
- Delta Lake ACID tables, schema enforcement and overwrite/append patterns
- Optional Databricks `OPTIMIZE` and `ZORDER BY` maintenance
- Logging, configuration, reusable transformations and basic tests

## Dataset

The included sample data is synthetic. It contains:
- `customers.csv`: customer master data
- `products.csv`: product master data
- `orders.csv`: order-line transactions, including a few intentionally invalid/duplicate rows for data-quality practice

The pipeline does not require downloading data.

## Run locally

Recommended: Python 3.10 or 3.11, Java 11 or 17.

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

pip install -r requirements.txt
python generate_data.py
python run_pipeline.py
```

The local run writes Delta tables under `output/`. Delta Lake and Spark versions must be compatible; the versions in `requirements.txt` are pinned together.

If your environment has difficulty resolving Delta dependencies, use the Databricks instructions below. Delta Lake is already available in Databricks Runtime with Delta support.

## Run on Databricks

1. Upload the project folder (or the CSV files) to your workspace/Volume.
2. Create a cluster using a current Databricks Runtime with Delta Lake support.
3. Put the CSVs in a folder such as `/Volumes/<catalog>/<schema>/<volume>/pyspark_etl/data/`.
4. In `config.py`, set `INPUT_DIR` to that folder and `OUTPUT_DIR` to a writable Volume path, or pass paths as environment variables.
5. Run `python run_pipeline.py` from a Repo, or copy the source files into a notebook and execute the pipeline entry point.

Example environment variables:

```bash
INPUT_DIR=/Volumes/main/default/project_data
OUTPUT_DIR=/Volumes/main/default/project_output
```

For DBFS-based workspaces, paths such as `dbfs:/FileStore/project_data` can be used when supported by the cluster/runtime.

## Output tables

| Layer | Table/folder | Purpose |
|---|---|---|
| Bronze | `bronze_orders`, `bronze_customers`, `bronze_products` | Raw source values + ingestion metadata |
| Silver | `silver_orders`, `silver_customers`, `silver_products` | Typed, standardized, deduplicated records |
| Silver | `quarantine_orders` | Invalid order rows and rejection reasons |
| Gold | `gold_daily_sales` | Revenue, order count and units by date |
| Gold | `gold_product_performance` | Revenue and units by product/category |
| Gold | `gold_customer_summary` | Customer-level order and spend metrics |

All tables are Delta format. Partitioning is applied to date-oriented outputs where appropriate. Avoid partitioning tiny tables or using very high-cardinality columns.

## Data quality rules

Orders are accepted only when:
- `order_id`, `customer_id`, `product_id`, `order_date`, `quantity`, and `unit_price` are present
- quantity is greater than zero
- unit price is non-negative
- the date can be parsed
- referenced customer and product keys exist in the master data

Duplicate `order_id` records are reduced to one deterministic row. Invalid rows are written to quarantine with a `reject_reason`.

## Interview walkthrough (2–3 minutes)

1. **Bronze:** I ingest source CSVs with explicit schemas and add source file and ingestion timestamp. I retain the source values for traceability.
2. **Silver:** I standardize strings, cast data types, deduplicate business keys, apply validation rules, and separate rejected rows into quarantine.
3. **Joins:** I enrich order facts with customer and product dimensions using joins, and identify unmatched foreign keys as data-quality failures.
4. **Gold:** I aggregate clean transactions into daily sales, product performance, and customer summaries.
5. **Performance:** I partition large date-based tables by year/month, repartition before expensive writes when justified, cache only reused data, and use Delta `OPTIMIZE`/`ZORDER` on Databricks.
6. **Reliability:** I keep the pipeline idempotent by overwriting the project output for this demo. In production, I would use incremental watermarks and Delta `MERGE` for upserts.

## Important production considerations

- This is a batch portfolio project, not a streaming pipeline.
- The demo uses overwrite mode for repeatable runs. For production incremental loads, use a persisted watermark and Delta `MERGE`.
- `OPTIMIZE` and `ZORDER BY` are Databricks SQL extensions. The code attempts them only when enabled and logs a warning if unavailable.
- For very large datasets, tune shuffle partitions based on cluster size and data volume; do not blindly increase partitions.
