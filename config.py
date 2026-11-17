import os

INPUT_DIR = os.getenv("INPUT_DIR", "data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
ENABLE_DATABRICKS_OPTIMIZE = os.getenv("ENABLE_DATABRICKS_OPTIMIZE", "false").lower() == "true"
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "8")
