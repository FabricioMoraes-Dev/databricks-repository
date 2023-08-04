# Databricks notebook source
!python -m pip install --upgrade pip

# COMMAND ----------

pip install requests

# COMMAND ----------

link_api = "https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo?ano=2023&periodicidade=TRIMESTRAL&periodo=1&tipo=Bancos+e+financeiras"

# COMMAND ----------

import pandas as pd

dados = pd.read_csv(link_api, sep=';', encoding='latin-1')

# COMMAND ----------

df = spark.createDataFrame(dados)

# COMMAND ----------

df.display()
df.count()

# COMMAND ----------

df2 = df.drop("Trimestre","Unnamed: 14")
df2.display()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col


df3 =  df2.select(col("Ano").cast("integer").alias("ano"),
                  col("Categoria").alias("categoria"),
                  col("CNPJ IF").cast("integer").alias("cnpj"),
                  col("Instituição financeira").alias("instituicao_financeira"),
                  col("Índice").alias("indice"),
                  col("Quantidade de reclamações reguladas procedentes").cast("integer").alias("qtd_procedentes"),
                  col("Quantidade de reclamações reguladas - outras").cast("integer").alias("qtd_outras"),
                  col("Quantidade de reclamações não reguladas").cast("integer").alias("qtd_Nreguladas"),
                  col("Quantidade total de reclamações").cast("integer").alias("qtd_total"),
                  col("Quantidade total de clientes  CCS e SCR").cast("integer").alias("qtd_total_clientes"),
                  col("Quantidade de clientes  CCS").cast("integer").alias("qtd_total_ccs"),
                  col("Quantidade de clientes  SCR").cast("integer").alias("qtd_total_scr")
                  )

# COMMAND ----------

df3.printSchema()
df3.count()

# COMMAND ----------

df3.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df4 = df3.withColumn("indice_2", regexp_replace("indice", ",","."))
df4.display()

# COMMAND ----------

df5 = df4.select(df4.ano,
                 df4.categoria,
                 df4.cnpj,
                 df4.instituicao_financeira,
                 df4.indice,
                 col("indice_2").cast("float"),
                 df4.qtd_procedentes,
                 df4.qtd_outras,
                 df4.qtd_Nreguladas,
                 df4.qtd_total,
                 df4.qtd_total_clientes,
                 df4.qtd_total_ccs,
                 df4.qtd_total_scr,
    )
df5.display()

# COMMAND ----------

df_final = df5.coalesce(1)

# COMMAND ----------

df_final.write.parquet('/mnt/fab_mount/if2023tr1_transformed')

# COMMAND ----------

dbutils.fs.ls('/mnt/fab_mount/')
