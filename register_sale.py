from delta import *
import pyspark
from schemas import pharmacy_sales_schema

builder = (
    pyspark.sql.SparkSession.builder
    .appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") 
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Reading the sales_erp file with the sales of the day
df = spark.read.format("csv").option("header", True).schema(pharmacy_sales_schema).load("sales_erp.csv")
df.show()

# Appending the data to the /sales/daily_sales table
df.write.format("delta").mode("append").save("/sales/daily_sales")
del df

df = spark.read.format("delta").load("/sales/daily_sales")
df.show()

spark.stop()