from src.spark_session import create_spark
from src.pipeline import run_pipeline

if __name__ == "__main__":
    spark = create_spark()
    try:
        metrics = run_pipeline(spark)
        print("\nPipeline metrics:")
        metrics.show(truncate=False)
    finally:
        spark.stop()
