"""
AWS Glue Job — Ingestão Raw → Bronze
Pipeline de Alfabetização - Tech Challenge Fase 2

Este script ingere os 5 CSVs da camada raw (upload manual) para a camada Bronze
em formato Parquet particionado, adicionando metadados de ingestão.

Fontes:
1. meta_alfabetizacao_brasil  — metas nacionais 2024-2030
2. meta_alfabetizacao_uf      — metas por estado 2024-2030
3. meta_alfabetizacao_municipio — metas por município 2024-2030
4. avaliacao_uf               — resultados por estado (taxa, média, níveis 0-8)
5. avaliacao_municipio        — resultados por município (taxa, média, níveis 0-8)

Como usar:
- Faça upload dos CSVs para: s3://tc2-alfabetizacao-datalake/raw-csv/
- Crie um Glue Job (Python Shell ou Spark) e cole este script
- Execute o job
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import lit, current_timestamp

# Inicialização do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# CONFIGURAÇÃO - Ajuste o nome do bucket aqui
# ============================================================
BUCKET = "tc2-alfabetizacao-datalake"
RAW_PATH = f"s3://{BUCKET}/raw-csv"
BRONZE_PATH = f"s3://{BUCKET}/bronze"


# ============================================================
# FUNÇÃO GENÉRICA DE INGESTÃO
# ============================================================
def ingerir_para_bronze(nome_arquivo_csv, nome_fonte, particao_campo="ano"):
    """
    Lê um CSV da camada raw, adiciona metadados e grava em Parquet na Bronze.
    
    Args:
        nome_arquivo_csv: Nome do arquivo CSV no S3 (sem extensão .csv se a pasta já tem)
        nome_fonte: Nome descritivo da fonte (usado como subpasta na Bronze)
        particao_campo: Campo para particionamento (default: "ano")
    """
    print(f"\n{'='*60}")
    print(f"Ingerindo: {nome_fonte}")
    print(f"{'='*60}")
    
    # Ler CSV
    input_path = f"{RAW_PATH}/{nome_arquivo_csv}"
    print(f"Lendo de: {input_path}")
    
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(input_path)
    
    print(f"Registros lidos: {df.count()}")
    print(f"Colunas: {df.columns}")
    
    # Adicionar metadados de ingestão
    df = df.withColumn("_timestamp_ingestao", current_timestamp()) \
           .withColumn("_fonte", lit("base_dos_dados")) \
           .withColumn("_arquivo_original", lit(nome_arquivo_csv))
    
    # Gravar em Parquet particionado
    output_path = f"{BRONZE_PATH}/{nome_fonte}/"
    print(f"Gravando em: {output_path}")
    
    df.write \
        .mode("overwrite") \
        .partitionBy(particao_campo) \
        .parquet(output_path)
    
    print(f"✓ {nome_fonte} ingerido com sucesso!")
    return df.count()


# ============================================================
# 1. META ALFABETIZAÇÃO BRASIL
# Colunas: ano, rede, taxa_alfabetizacao, meta_2024..2030, percentual_participacao
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    nome_fonte="meta_brasil"
)


# ============================================================
# 2. META ALFABETIZAÇÃO UF (ESTADOS)
# Colunas: ano, sigla_uf, rede, taxa_alfabetizacao, meta_2024..2030, percentual_participacao
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    nome_fonte="meta_uf"
)


# ============================================================
# 3. META ALFABETIZAÇÃO MUNICÍPIO
# Colunas: ano, id_municipio, rede, taxa_alfabetizacao, meta_2024..2030, 
#          nivel_alfabetizacao, percentual_participacao
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
    nome_fonte="meta_municipio"
)


# ============================================================
# 4. AVALIAÇÃO ALFABETIZAÇÃO UF (ESTADOS)
# Colunas: ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues,
#          proporcao_aluno_nivel_0..nivel_8
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_uf.csv",
    nome_fonte="avaliacao_uf"
)


# ============================================================
# 5. AVALIAÇÃO ALFABETIZAÇÃO MUNICÍPIO
# Colunas: ano, id_municipio, serie, rede, taxa_alfabetizacao, media_portugues,
#          proporcao_aluno_nivel_0..nivel_8
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_municipio.csv",
    nome_fonte="avaliacao_municipio"
)


# ============================================================
# FINALIZAÇÃO
# ============================================================
print("\n" + "="*60)
print("INGESTÃO BRONZE COMPLETA!")
print("="*60)
print(f"\nDados gravados em: s3://{BUCKET}/bronze/")
print("Subpastas criadas:")
print("  - bronze/meta_brasil/")
print("  - bronze/meta_uf/")
print("  - bronze/meta_municipio/")
print("  - bronze/avaliacao_uf/")
print("  - bronze/avaliacao_municipio/")

job.commit()
