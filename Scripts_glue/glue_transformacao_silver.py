"""
AWS Glue Job - Transformacao Bronze -> Silver
Pipeline de Alfabetizacao - Tech Challenge Fase 2

Tabelas processadas:
  1. meta_brasil         -> Silver/Meta_brasil/
  2. meta_uf             -> Silver/Meta_uf/
  3. meta_municipio      -> Silver/Meta_municipio/
  4. avaliacao_uf        -> Silver/Avaliacao_uf/
  5. avaliacao_municipio -> Silver/Avaliacao_municipio/
  6. ts_aluno (streaming)-> Silver/Ts_aluno/
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, upper, trim, when, lit, current_timestamp
)
from pyspark.sql.types import IntegerType, DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "tech-challange-fase2"
BRONZE_PATH = f"s3://{BUCKET}/Bronze"
SILVER_PATH = f"s3://{BUCKET}/Silver"

def padronizar_rede(df):
    return df.withColumn("rede",
        when(upper(trim(col("rede"))).isin("PUBLICA", "PÚBLICA"), "Publica")
        .when(upper(trim(col("rede"))).isin("MUNICIPAL"), "Municipal")
        .when(upper(trim(col("rede"))).isin("ESTADUAL"), "Estadual")
        .when(upper(trim(col("rede"))).isin("FEDERAL"), "Federal")
        .when(upper(trim(col("rede"))).isin("PRIVADA"), "Privada")
        .otherwise(trim(col("rede")))
    )

def padronizar_sigla_uf(df):
    return df.withColumn("sigla_uf", upper(trim(col("sigla_uf"))))

def cast_metas(df):
    for c in [x for x in df.columns if x.startswith("meta_alfabetizacao_")]:
        df = df.withColumn(c, col(c).cast(DoubleType()))
    return df

def adicionar_metadados_silver(df):
    return df.withColumn("_timestamp_silver", current_timestamp())

colunas_remover = ["timestamp_ingestao", "fonte", "arquivo_original",
                   "_timestamp_ingestao", "_fonte", "_arquivo_original"]

# ============================================================
# 1. META BRASIL
# ============================================================
print("\n" + "="*60)
print("1/6 - SILVER: meta_brasil")
print("="*60)
df = spark.read.parquet(f"{BRONZE_PATH}/Meta_brasil/")
df = df.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))
df = df.withColumn("percentual_participacao", col("percentual_participacao").cast(DoubleType()))
df = padronizar_rede(df)
df = cast_metas(df)
df = df.withColumn("ano", col("ano").cast(IntegerType()))
df = df.filter(col("ano").isNotNull() & col("rede").isNotNull())
df = df.dropDuplicates(["ano", "rede"])
df = df.filter((col("taxa_alfabetizacao").isNull()) | ((col("taxa_alfabetizacao") >= 0) & (col("taxa_alfabetizacao") <= 100)))
for c in colunas_remover:
    if c in df.columns:
        df = df.drop(c)
df = adicionar_metadados_silver(df)
print(f"Registros: {df.count()}")
df.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER_PATH}/Meta_brasil/")
print("OK meta_brasil")

# ============================================================
# 2. META UF
# ============================================================
print("\n" + "="*60)
print("2/6 - SILVER: meta_uf")
print("="*60)
df = spark.read.parquet(f"{BRONZE_PATH}/Meta_uf/")
df = padronizar_sigla_uf(df)
df = padronizar_rede(df)
df = cast_metas(df)
df = df.withColumn("ano", col("ano").cast(IntegerType()))
df = df.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))
df = df.withColumn("percentual_participacao", col("percentual_participacao").cast(DoubleType()))
df = df.filter(col("ano").isNotNull() & col("sigla_uf").isNotNull() & col("rede").isNotNull())
df = df.dropDuplicates(["ano", "sigla_uf", "rede"])
df = df.filter((col("taxa_alfabetizacao").isNull()) | ((col("taxa_alfabetizacao") >= 0) & (col("taxa_alfabetizacao") <= 100)))
for c in colunas_remover:
    if c in df.columns:
        df = df.drop(c)
df = adicionar_metadados_silver(df)
print(f"Registros: {df.count()}")
df.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER_PATH}/Meta_uf/")
print("OK meta_uf")

# ============================================================
# 3. META MUNICIPIO
# ============================================================
print("\n" + "="*60)
print("3/6 - SILVER: meta_municipio")
print("="*60)
df = spark.read.parquet(f"{BRONZE_PATH}/Meta_municipio/")
df = padronizar_rede(df)
df = cast_metas(df)
df = df.withColumn("ano", col("ano").cast(IntegerType()))
df = df.withColumn("id_municipio", col("id_municipio").cast(IntegerType()))
df = df.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))
df = df.withColumn("percentual_participacao", col("percentual_participacao").cast(DoubleType()))
df = df.withColumn("nivel_alfabetizacao", col("nivel_alfabetizacao").cast(IntegerType()))
df = df.filter(col("ano").isNotNull() & col("id_municipio").isNotNull() & col("rede").isNotNull())
df = df.dropDuplicates(["ano", "id_municipio", "rede"])
df = df.filter((col("taxa_alfabetizacao").isNull()) | ((col("taxa_alfabetizacao") >= 0) & (col("taxa_alfabetizacao") <= 100)))
for c in colunas_remover:
    if c in df.columns:
        df = df.drop(c)
df = adicionar_metadados_silver(df)
print(f"Registros: {df.count()}")
df.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER_PATH}/Meta_municipio/")
print("OK meta_municipio")

# ============================================================
# 4. AVALIACAO UF
# ============================================================
print("\n" + "="*60)
print("4/6 - SILVER: avaliacao_uf")
print("="*60)
df = spark.read.parquet(f"{BRONZE_PATH}/Avaliacao_uf/")
df = padronizar_sigla_uf(df)
df = df.withColumn("ano", col("ano").cast(IntegerType()))
df = df.withColumn("serie", col("serie").cast(IntegerType()))
df = df.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))
df = df.withColumn("media_portugues", col("media_portugues").cast(DoubleType()))
for i in range(9):
    cn = f"proporcao_aluno_nivel_{i}"
    if cn in df.columns:
        df = df.withColumn(cn, col(cn).cast(DoubleType()))
df = df.withColumn("rede",
    when(col("rede") == "2", "Estadual")
    .when(col("rede") == "3", "Municipal")
    .when(col("rede") == "5", "Publica")
    .when(col("rede") == "0", "Total")
    .otherwise(col("rede"))
)
df = df.filter(col("ano").isNotNull() & col("sigla_uf").isNotNull() & col("rede").isNotNull())
df = df.dropDuplicates(["ano", "sigla_uf", "serie", "rede"])
df = df.filter((col("taxa_alfabetizacao").isNull()) | ((col("taxa_alfabetizacao") >= 0) & (col("taxa_alfabetizacao") <= 100)))
for c in colunas_remover:
    if c in df.columns:
        df = df.drop(c)
df = adicionar_metadados_silver(df)
print(f"Registros: {df.count()}")
df.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER_PATH}/Avaliacao_uf/")
print("OK avaliacao_uf")

# ============================================================
# 5. AVALIACAO MUNICIPIO
# ============================================================
print("\n" + "="*60)
print("5/6 - SILVER: avaliacao_municipio")
print("="*60)
df = spark.read.parquet(f"{BRONZE_PATH}/Avaliacao_municipio/")
df = df.withColumn("ano", col("ano").cast(IntegerType()))
df = df.withColumn("id_municipio", col("id_municipio").cast(IntegerType()))
df = df.withColumn("serie", col("serie").cast(IntegerType()))
df = df.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))
df = df.withColumn("media_portugues", col("media_portugues").cast(DoubleType()))
for i in range(9):
    cn = f"proporcao_aluno_nivel_{i}"
    if cn in df.columns:
        df = df.withColumn(cn, col(cn).cast(DoubleType()))
df = df.withColumn("rede",
    when(col("rede") == "2", "Estadual")
    .when(col("rede") == "3", "Municipal")
    .when(col("rede") == "5", "Publica")
    .when(col("rede") == "0", "Total")
    .otherwise(col("rede"))
)
df = df.filter(col("ano").isNotNull() & col("id_municipio").isNotNull() & col("rede").isNotNull())
df = df.dropDuplicates(["ano", "id_municipio", "serie", "rede"])
df = df.filter((col("taxa_alfabetizacao").isNull()) | ((col("taxa_alfabetizacao") >= 0) & (col("taxa_alfabetizacao") <= 100)))
for c in colunas_remover:
    if c in df.columns:
        df = df.drop(c)
df = adicionar_metadados_silver(df)
print(f"Registros: {df.count()}")
df.write.mode("overwrite").partitionBy("ano").parquet(f"{SILVER_PATH}/Avaliacao_municipio/")
print("OK avaliacao_municipio")


# ============================================================
# 6. TS_ALUNO (STREAMING)
# ============================================================
print("\n" + "="*60)
print("6/6 - SILVER: Ts_aluno (streaming)")
print("="*60)

try:
    # Ler TODOS os Parquets recursivamente
    # Path real: Bronze/Ts_aluno/data_ingestao=YYYY-MM-DD/*.parquet
    # Colunas em MINUSCULO conforme gravado pela Lambda consumidora
    df = spark.read.option("recursiveFileLookup", "true").parquet(f"{BRONZE_PATH}/Ts_aluno/")

    print(f"Colunas encontradas: {df.columns}")
    print(f"Total lidos: {df.count()}")

    # Cast dos campos (nomes em minusculo)
    df = df.withColumn("nu_ano_avaliacao", col("nu_ano_avaliacao").cast(IntegerType()))
    df = df.withColumn("co_uf", col("co_uf").cast(IntegerType()))
    df = df.withColumn("sg_uf", upper(trim(col("sg_uf"))))
    df = df.withColumn("id_aluno", col("id_aluno").cast("long"))
    df = df.withColumn("tp_serie", col("tp_serie").cast(IntegerType()))
    df = df.withColumn("id_escola", col("id_escola").cast("long"))
    df = df.withColumn("tp_dependencia", col("tp_dependencia").cast(IntegerType()))
    df = df.withColumn("co_municipio", col("co_municipio").cast(IntegerType()))
    df = df.withColumn("in_presenca_lp", col("in_presenca_lp").cast(IntegerType()))
    df = df.withColumn("in_preenchimento_lp", col("in_preenchimento_lp").cast(IntegerType()))
    df = df.withColumn("co_caderno_lp", col("co_caderno_lp").cast(IntegerType()))
    df = df.withColumn("co_bloco_1", col("co_bloco_1").cast(IntegerType()))
    df = df.withColumn("co_bloco_2", col("co_bloco_2").cast(IntegerType()))
    df = df.withColumn("co_bloco_3", col("co_bloco_3").cast(IntegerType()))
    df = df.withColumn("co_bloco_4", col("co_bloco_4").cast(IntegerType()))
    df = df.withColumn("vl_peso_aluno_lp", col("vl_peso_aluno_lp").cast(DoubleType()))
    df = df.withColumn("vl_proficiencia_lp", col("vl_proficiencia_lp").cast(DoubleType()))
    df = df.withColumn("in_alfabetizado", col("in_alfabetizado").cast(IntegerType()))

    # Remover sem chave
    df = df.filter(
        col("nu_ano_avaliacao").isNotNull() &
        col("id_aluno").isNotNull() &
        col("co_uf").isNotNull()
    )

    # Validar tp_dependencia
    df = df.filter(col("tp_dependencia").isin(1, 2, 3, 4))

    # Validar proficiencia em range
    df = df.filter(
        (col("vl_proficiencia_lp").isNull()) |
        ((col("vl_proficiencia_lp") >= 400) & (col("vl_proficiencia_lp") <= 1000))
    )

    # Deduplicar
    df = df.dropDuplicates(["nu_ano_avaliacao", "id_aluno"])

    # Remover colunas de metadados da bronze
    for c in ["_timestamp_ingestao", "_fonte", "_arquivo_original", "data_ingestao"]:
        if c in df.columns:
            df = df.drop(c)

    df = adicionar_metadados_silver(df)

    print(f"Registros apos limpeza: {df.count()}")
    df.write.mode("overwrite").parquet(f"{SILVER_PATH}/Ts_aluno/")
    print("OK Ts_aluno gravado na Silver")

except Exception as e:
    print(f"ERRO Ts_aluno: {str(e)}")
    print("Pulando transformacao de alunos.")


# ============================================================
# FINALIZACAO
# ============================================================
print("\n" + "="*60)
print("TRANSFORMACAO BRONZE -> SILVER COMPLETA!")
print("="*60)

job.commit()
