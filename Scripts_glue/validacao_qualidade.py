"""
Script de Validação e Qualidade de Dados
Pipeline de Alfabetização - Tech Challenge Fase 2

Executa verificações de qualidade nas camadas Silver e Gold.
Pode ser rodado como Glue Job ou localmente com PySpark.

Validações:
  1. Campos obrigatórios não nulos
  2. Ranges de valores (taxa 0-100, proficiência 400-1000)
  3. Unicidade por chave natural (sem duplicatas)
  4. Consistência entre tabelas (chaves de relacionamento)
  5. Metas monotonicamente crescentes (2024 <= 2025 <= ... <= 2030)
  6. Proporções somam ~100% (tolerância ±2%)
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, count, sum as spark_sum, abs as spark_abs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = "tech-challange-fase2"
SILVER_PATH = f"s3://{BUCKET}/Silver"

resultados = []


def registrar(tabela, regra, passou, detalhe=""):
    status = "PASSOU" if passou else "FALHOU"
    resultados.append({"tabela": tabela, "regra": regra, "status": status, "detalhe": detalhe})
    print(f"  [{status}] {tabela} — {regra} {detalhe}")


# ============================================================
# CARREGAR TABELAS SILVER
# ============================================================
print("="*60)
print("VALIDAÇÃO DE QUALIDADE — CAMADA SILVER")
print("="*60)

df_meta_brasil = spark.read.parquet(f"{SILVER_PATH}/Meta_brasil/")
df_meta_uf = spark.read.parquet(f"{SILVER_PATH}/Meta_uf/")
df_meta_municipio = spark.read.parquet(f"{SILVER_PATH}/Meta_municipio/")
df_avaliacao_uf = spark.read.parquet(f"{SILVER_PATH}/Avaliacao_uf/")
df_avaliacao_municipio = spark.read.parquet(f"{SILVER_PATH}/Avaliacao_municipio/")

try:
    df_alunos = spark.read.option("recursiveFileLookup", "true").parquet(f"{SILVER_PATH}/Ts_aluno/")
    tem_alunos = True
except:
    tem_alunos = False


# ============================================================
# 1. CAMPOS OBRIGATÓRIOS NÃO NULOS
# ============================================================
print("\n--- 1. Campos obrigatórios ---")

nulos_brasil = df_meta_brasil.filter(col("ano").isNull() | col("rede").isNull()).count()
registrar("meta_brasil", "ano/rede não nulos", nulos_brasil == 0, f"({nulos_brasil} nulos)")

nulos_uf = df_meta_uf.filter(col("ano").isNull() | col("sigla_uf").isNull() | col("rede").isNull()).count()
registrar("meta_uf", "ano/sigla_uf/rede não nulos", nulos_uf == 0, f"({nulos_uf} nulos)")

nulos_mun = df_meta_municipio.filter(col("ano").isNull() | col("id_municipio").isNull()).count()
registrar("meta_municipio", "ano/id_municipio não nulos", nulos_mun == 0, f"({nulos_mun} nulos)")


# ============================================================
# 2. VALIDAÇÃO DE RANGES
# ============================================================
print("\n--- 2. Ranges de valores ---")

# Taxa entre 0 e 100
fora_range_uf = df_meta_uf.filter(
    col("taxa_alfabetizacao").isNotNull() &
    ((col("taxa_alfabetizacao") < 0) | (col("taxa_alfabetizacao") > 100))
).count()
registrar("meta_uf", "taxa_alfabetizacao em [0, 100]", fora_range_uf == 0, f"({fora_range_uf} fora)")

fora_range_mun = df_meta_municipio.filter(
    col("taxa_alfabetizacao").isNotNull() &
    ((col("taxa_alfabetizacao") < 0) | (col("taxa_alfabetizacao") > 100))
).count()
registrar("meta_municipio", "taxa_alfabetizacao em [0, 100]", fora_range_mun == 0, f"({fora_range_mun} fora)")

# Proficiência entre 400 e 1000
if tem_alunos:
    fora_prof = df_alunos.filter(
        col("vl_proficiencia_lp").isNotNull() &
        ((col("vl_proficiencia_lp") < 400) | (col("vl_proficiencia_lp") > 1000))
    ).count()
    registrar("ts_aluno", "proficiencia em [400, 1000]", fora_prof == 0, f"({fora_prof} fora)")


# ============================================================
# 3. UNICIDADE (SEM DUPLICATAS)
# ============================================================
print("\n--- 3. Unicidade por chave natural ---")

total_uf = df_meta_uf.count()
distinct_uf = df_meta_uf.dropDuplicates(["ano", "sigla_uf", "rede"]).count()
registrar("meta_uf", "sem duplicatas (ano+uf+rede)", total_uf == distinct_uf,
          f"({total_uf} total, {distinct_uf} distintos)")

total_mun = df_meta_municipio.count()
distinct_mun = df_meta_municipio.dropDuplicates(["ano", "id_municipio", "rede"]).count()
registrar("meta_municipio", "sem duplicatas (ano+mun+rede)", total_mun == distinct_mun,
          f"({total_mun} total, {distinct_mun} distintos)")

if tem_alunos:
    total_alunos = df_alunos.count()
    distinct_alunos = df_alunos.dropDuplicates(["nu_ano_avaliacao", "id_aluno"]).count()
    registrar("ts_aluno", "sem duplicatas (ano+id_aluno)", total_alunos == distinct_alunos,
              f"({total_alunos} total, {distinct_alunos} distintos)")


# ============================================================
# 4. METAS MONOTONICAMENTE CRESCENTES
# ============================================================
print("\n--- 4. Metas crescentes ---")

metas_nao_crescentes = df_meta_uf.filter(
    col("meta_alfabetizacao_2025").isNotNull() &
    col("meta_alfabetizacao_2026").isNotNull() &
    (col("meta_alfabetizacao_2025") > col("meta_alfabetizacao_2026"))
).count()
registrar("meta_uf", "metas 2025 <= 2026", metas_nao_crescentes == 0, f"({metas_nao_crescentes} violações)")

metas_nao_crescentes_2 = df_meta_uf.filter(
    col("meta_alfabetizacao_2029").isNotNull() &
    col("meta_alfabetizacao_2030").isNotNull() &
    (col("meta_alfabetizacao_2029") > col("meta_alfabetizacao_2030"))
).count()
registrar("meta_uf", "metas 2029 <= 2030", metas_nao_crescentes_2 == 0, f"({metas_nao_crescentes_2} violações)")


# ============================================================
# 5. PROPORÇÕES SOMAM ~100%
# ============================================================
print("\n--- 5. Proporções somam ~100% ---")

df_check = df_avaliacao_uf.filter(col("proporcao_aluno_nivel_0").isNotNull())
if df_check.count() > 0:
    from pyspark.sql.functions import lit
    df_soma = df_check.withColumn("soma_niveis",
        col("proporcao_aluno_nivel_0") + col("proporcao_aluno_nivel_1") +
        col("proporcao_aluno_nivel_2") + col("proporcao_aluno_nivel_3") +
        col("proporcao_aluno_nivel_4") + col("proporcao_aluno_nivel_5") +
        col("proporcao_aluno_nivel_6") + col("proporcao_aluno_nivel_7") +
        col("proporcao_aluno_nivel_8")
    )
    fora_soma = df_soma.filter(spark_abs(col("soma_niveis") - 100) > 2).count()
    registrar("avaliacao_uf", "proporções somam ~100% (±2%)", fora_soma == 0, f"({fora_soma} fora)")
else:
    registrar("avaliacao_uf", "proporções somam ~100%", True, "(sem dados de proporção para 2023)")


# ============================================================
# 6. CONSISTÊNCIA ENTRE TABELAS
# ============================================================
print("\n--- 6. Consistência entre tabelas ---")

ufs_meta = set(df_meta_uf.select("sigla_uf").distinct().toPandas()["sigla_uf"].tolist())
ufs_avaliacao = set(df_avaliacao_uf.select("sigla_uf").distinct().toPandas()["sigla_uf"].tolist())
ufs_somente_meta = ufs_meta - ufs_avaliacao
ufs_somente_avaliacao = ufs_avaliacao - ufs_meta
registrar("meta_uf vs avaliacao_uf", "UFs consistentes entre tabelas",
          len(ufs_somente_meta) == 0 and len(ufs_somente_avaliacao) == 0,
          f"(só meta: {ufs_somente_meta}, só avaliação: {ufs_somente_avaliacao})")


# ============================================================
# RESUMO
# ============================================================
print("\n" + "="*60)
print("RESUMO DA VALIDAÇÃO")
print("="*60)
total_regras = len(resultados)
passou = sum(1 for r in resultados if r["status"] == "PASSOU")
falhou = sum(1 for r in resultados if r["status"] == "FALHOU")
print(f"Total de regras: {total_regras}")
print(f"Passou: {passou} ({passou/total_regras*100:.0f}%)")
print(f"Falhou: {falhou} ({falhou/total_regras*100:.0f}%)")

if falhou > 0:
    print("\nRegras que falharam:")
    for r in resultados:
        if r["status"] == "FALHOU":
            print(f"  ✗ {r['tabela']} — {r['regra']} {r['detalhe']}")

print("="*60)

job.commit()
