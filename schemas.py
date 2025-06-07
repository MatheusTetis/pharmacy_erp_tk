from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType, LongType
)

# Define the schema for the pharmacy selling table
pharmacy_sales_schema = StructType([
    StructField("cart_id", StringType(), False),  # Unique identifier for each sales transaction
    StructField("transaction_id", StringType(), False),  # Unique identifier for each sales transaction
    StructField("sale_datetime", TimestampType(), False), # Date and time when the sale occurred
    StructField("industry_name", StringType(), False), # Date and time when the sale occurred
    StructField("product_id", StringType(), False),      # Unique identifier for the product sold
    StructField("product_name", StringType(), False),    # Name of the product sold
    StructField("unit_price", DoubleType(), False),      # Price of a single unit of the product
    StructField("discount_applied", DoubleType(), True),  # Optional: Amount of discount applied to the product (e.g., 0.05 for 5% off)
    StructField("liquid_price", DoubleType(), False),     # Total price for the product (quantity * unit_price, after discounts)
    StructField("taxes_price", DoubleType(), False),     # Total price for the product (quantity * unit_price, after discounts)
    StructField("liquid_price_after_taxes", DoubleType(), False),     # Total price for the product (quantity * unit_price, after discounts)
    StructField("quantity", IntegerType(), False),       # Number of units of the product sold
    StructField("total_price", DoubleType(), False),     # Total price for the product (quantity * unit_price, after discounts)
    StructField("total_price_after_taxes", DoubleType(), False),     # Total price for the product (quantity * unit_price, after discounts)
])

dim_product_schema = StructType([
    StructField("id", StringType(), False),
    StructField("industry_name", StringType(), False),
    StructField("industry_cnpj", StringType(), False),
    StructField("ggrem_code", StringType(), False),
    StructField("registro", StringType(), False),
    StructField("ean1", StringType(), False),
    StructField("ean2", StringType(), False),
    StructField("ean3", StringType(), False),
    StructField("substance_name", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("product_presentation", StringType(), False),
    StructField("terapy_class", StringType(), False),
    StructField("product_type", StringType(), False),
    StructField("price_regime", StringType(), False),
    StructField("unit_price", StringType(), False),
    StructField("unit_price_after_taxes", StringType(), False),
    StructField("hospital_restriction", StringType(), False),
    StructField("cap", StringType(), False),
    StructField("confaz87", StringType(), False),
    StructField("icms0", StringType(), False),
    StructField("appeal_analysis", StringType(), False),
    StructField("credit_concession", StringType(), False),
    StructField("stripe", StringType(), False),
])
