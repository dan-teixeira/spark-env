# %%
import os
from dotenv import load_dotenv
from utils.SparkHelper import SessionBuilder
from delta.tables import DeltaTable
import pyspark.sql.functions as F

load_dotenv()

# %%

spark = (
    SessionBuilder.get_builder()
    .appName("template")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.shuffle.partitions", 200)
    .getOrCreate()
)

# %%

