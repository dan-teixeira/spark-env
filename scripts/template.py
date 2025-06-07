import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
from datetime import date
from utils.SparkHelper import SessionBuilder
from delta.tables import DeltaTable
import pyspark.sql.functions as F

# %%

# Load variables
load_dotenv()

# Script contants
EXECUTION_DATE = date.today()
APP_NAME = Path(__file__).stem

# Logging configuration
logger = logging.getLogger(APP_NAME)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s: %(message)s")
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# %%

spark = (
    SessionBuilder.get_builder()
    .appName(f"{APP_NAME} - {EXECUTION_DATE}")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.shuffle.partitions", 200)
    .getOrCreate()
)

logger.info("Spark session created")

# %%
