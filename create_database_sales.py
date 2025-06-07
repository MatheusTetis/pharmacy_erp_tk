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

# Creating the sales database into the default Catalog
spark.sql("CREATE DATABASE IF NOT EXISTS sales")

spark.stop()