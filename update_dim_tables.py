import pyspark.sql.functions as F
from uuid import uuid4

def update_dim_product(spark):
    # Loading the table from ANVISA
    anvisa_table = "TA_PRECO_MEDICAMENTO_GOV.csv"
    df_anvisa = spark.read.option("delimiter", ";").option("header", True).csv(anvisa_table)

    # Shaping it to the dim_products Schema and creating the id
    df_anvisa_formatted = (
        df_anvisa
        .withColumn("id", F.lit(str(uuid4())))
        .select(
            F.col("id"),
            F.col("LABORATÓRIO").alias("industry_name"),
            F.col("CNPJ").alias("industry_cnpj"),
            F.col("CÓDIGO GGREM").alias("ggrem_code"),
            F.col("REGISTRO").alias("registro"),
            F.col("EAN 1").alias("ean1"),
            F.col("EAN 2").alias("ean2"),
            F.col("EAN 3").alias("ean3"),
            F.col("SUBSTÂNCIA").alias("substance_name"),
            F.col("PRODUTO").alias("product_name"),
            F.col("APRESENTAÇÃO").alias("product_presentation"),
            F.col("CLASSE TERAPÊUTICA").alias("terapy_class"),
            F.col("TIPO DE PRODUTO (STATUS DO PRODUTO)").alias("product_type"),
            F.col("REGIME DE PREÇO").alias("price_regime"),
            F.col("PF Sem Impostos").alias("unit_price"),
            F.col("PF 12 %").alias("unit_price_after_taxes"),
            F.col("RESTRIÇÃO HOSPITALAR").alias("hospital_restriction"),
            F.col("CAP").alias("cap"),
            F.col("CONFAZ 87").alias("confaz87"),
            F.col("ICMS 0%").alias("icms0"),
            F.col("ANÁLISE RECURSAL").alias("appeal_analysis"),
            F.col("LISTA DE CONCESSÃO DE CRÉDITO TRIBUTÁRIO (PIS/COFINS)").alias("credit_concession"),
            F.col("TARJA").alias("stripe"),
        )
    )

    # If the dim_products is not yet created, let's create it
    df_anvisa_formatted.write.format("delta").mode("overwrite").save("/sales/dim_product")
    