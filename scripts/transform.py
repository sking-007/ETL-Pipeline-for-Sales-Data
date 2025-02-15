from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import pandas as pd

# Initialize Spark
spark = SparkSession.builder.appName("SalesDataTransformation").getOrCreate()

# Load raw sales data
raw_data_path = "data/raw_sales_data.csv"
df = spark.read.csv(raw_data_path, header=True, inferSchema=True)

# Handle missing values
df = df.fillna({'price': 0, 'quantity': 1})  # Default values

# Normalize: Convert price to float
df = df.withColumn("price", col("price").cast("float"))

# Convert Spark DataFrame back to Pandas for final processing
df_pandas = df.toPandas()

# Save transformed data
output_path = "data/processed_sales_data.csv"
df_pandas.to_csv(output_path, index=False)
print(f"Transformed data saved to {output_path}")
