import os
import pandas as pd
import sys
from pyspark.sql import SparkSession

# Manually update PYTHONPATH
# Initialize Spark session
spark = SparkSession.builder \
    .appName("YouTube Channel Scraping") \
    .getOrCreate()

# Read the parquet file
df = spark.read.parquet("s3a://andrew-huberman-podcast-analytics/comments/15R2pMqU2ok_43.parquet")
df.show()