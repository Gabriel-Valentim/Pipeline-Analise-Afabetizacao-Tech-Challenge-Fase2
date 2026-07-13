"""
AWS Glue Job — Transformação Silver → Gold
Pipeline de Alfabetização - Tech Challenge Fase 2

Este script lê os dados limpos da camada Silver, aplica agregações,
joins e cálculos analíticos, e grava na camada Gold.

Tabelas Gold criadas:
  1. panorama_alfabetizacao      — Taxa realizada vs meta, gap, atingimento (Brasil + UF + Município)
  2. evolucao_proficiencia       — Distribuição de alunos por nível de proficiência por UF e ano
  3. ranking_municipios          — Ranking de municípios por taxa de alfabetização dentro de cada UF
  4. indicadores_alunos          — Agregações dos microdados de alunos (streaming) por UF/município
  5. features_ml                 — Dataset pronto para treinamento de modelos preditivos
  6. desigualdade_regional       — Comparação entre regiões (Norte/Nordeste vs Sul/Sudeste/Centro-Oeste)
  7. municipios_criticos         — Municípios com maior gap para a meta 2030, classificados por criticidade
  8. tendencia_temporal          — Variação ano-a-ano por UF e projeção de atingimento da meta 2030

Como usar:
  - Crie um Glue Job (Spark) e cole este script
  - Execute após a transformação Silver estar completa
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    col, lit, when, round as spark_round, current_timestamp,
    count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    rank, percent_rank
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

# Inicialização do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# CONFIGURAÇÃO
# ============================================================
BUCKET = "tech-challange-fase2"
SILVER_PATH = f"s3://{BUCKET}/Silver"
GOLD_PATH = f"s3://{BUCKET}/Gold"


# ============================================================
# LEITURA DAS TABELAS SILVER
# ============================================================
print("Lendo tabelas Silver...")

df_meta_brasil = spark.read.parquet(f"{SILVER_PATH}/Meta_brasil/")
df_meta_uf = spark.read.parquet(f"{SILVER_PATH}/Meta_uf/")
df_meta_municipio = spark.read.parquet(f"{SILVER_PATH}/Meta_municipio/")
df_avaliacao_uf = spark.read.parquet(f"{SILVER_PATH}/Avaliacao_uf/")
df_avaliacao_municipio = spark.read.parquet(f"{SILVER_PATH}/Avaliacao_municipio/")

# Streaming (pode não existir ainda)
try:
    df_alunos = spark.read.parquet(f"{SILVER_PATH}/Ts_aluno/")
    tem_alunos = True
    print("OK Tabela Ts_aluno carregada")
except:
    tem_alunos = False
    print("Tabela Ts_aluno nao encontrada, pulando indicadores de alunos")

print("✓ Todas as tabelas Silver carregadas")


# ============================================================
# 1. PANORAMA ALFABETIZAÇÃO
#    Combina taxa realizada com meta para cada granularidade
# ============================================================
print("\n" + "="*60)
print("1/5 — GOLD: panorama_alfabetizacao")
print("="*60)

# --- Brasil ---
df_brasil = df_meta_brasil.select(
    col("ano"),
    lit("Brasil").alias("granularidade"),
    lit("BR").alias("codigo"),
    lit("Brasil").alias("nome"),
    col("rede"),
    col("taxa_alfabetizacao"),
    col("meta_alfabetizacao_2024"),
    col("meta_alfabetizacao_2025"),
    col("meta_alfabetizacao_2026"),
    col("meta_alfabetizacao_2027"),
    col("meta_alfabetizacao_2028"),
    col("meta_alfabetizacao_2029"),
    col("meta_alfabetizacao_2030"),
    col("percentual_participacao"),
)

# --- UF ---
df_uf_panorama = df_meta_uf.select(
    col("ano"),
    lit("UF").alias("granularidade"),
    col("sigla_uf").alias("codigo"),
    col("sigla_uf").alias("nome"),
    col("rede"),
    col("taxa_alfabetizacao"),
    col("meta_alfabetizacao_2024"),
    col("meta_alfabetizacao_2025"),
    col("meta_alfabetizacao_2026"),
    col("meta_alfabetizacao_2027"),
    col("meta_alfabetizacao_2028"),
    col("meta_alfabetizacao_2029"),
    col("meta_alfabetizacao_2030"),
    col("percentual_participacao"),
)

# --- Município ---
df_mun_panorama = df_meta_municipio.select(
    col("ano"),
    lit("Municipio").alias("granularidade"),
    col("id_municipio").cast("string").alias("codigo"),
    col("id_municipio").cast("string").alias("nome"),
    col("rede"),
    col("taxa_alfabetizacao"),
    col("meta_alfabetizacao_2024"),
    col("meta_alfabetizacao_2025"),
    col("meta_alfabetizacao_2026"),
    col("meta_alfabetizacao_2027"),
    col("meta_alfabetizacao_2028"),
    col("meta_alfabetizacao_2029"),
    col("meta_alfabetizacao_2030"),
    col("percentual_participacao"),
)

# Unir todas as granularidades
df_panorama = df_brasil.unionByName(df_uf_panorama).unionByName(df_mun_panorama)

# Calcular indicadores derivados
df_panorama = df_panorama.withColumn(
    "gap_meta_2030",
    when(col("taxa_alfabetizacao").isNotNull() & col("meta_alfabetizacao_2030").isNotNull(),
         spark_round(col("meta_alfabetizacao_2030") - col("taxa_alfabetizacao"), 2))
).withColumn(
    "percentual_atingimento_2030",
    when(col("taxa_alfabetizacao").isNotNull() & col("meta_alfabetizacao_2030").isNotNull() & (col("meta_alfabetizacao_2030") > 0),
         spark_round(col("taxa_alfabetizacao") / col("meta_alfabetizacao_2030") * 100, 2))
).withColumn(
    "meta_atingida_2030",
    when(col("taxa_alfabetizacao") >= col("meta_alfabetizacao_2030"), lit(1)).otherwise(lit(0))
).withColumn("_timestamp_gold", current_timestamp())

print(f"Registros: {df_panorama.count()}")
df_panorama.write.mode("overwrite").partitionBy("ano", "granularidade").parquet(f"{GOLD_PATH}/panorama_alfabetizacao/")
print("✓ panorama_alfabetizacao gravado na Gold")


# ============================================================
# 2. EVOLUÇÃO PROFICIÊNCIA
#    Distribuição de alunos por nível de proficiência por UF e ano
# ============================================================
print("\n" + "="*60)
print("2/5 — GOLD: evolucao_proficiencia")
print("="*60)

colunas_nivel = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
colunas_existentes = [c for c in colunas_nivel if c in df_avaliacao_uf.columns]

df_evolucao = df_avaliacao_uf.select(
    "ano", "sigla_uf", "serie", "rede",
    "taxa_alfabetizacao", "media_portugues",
    *colunas_existentes
).withColumn("_timestamp_gold", current_timestamp())

print(f"Registros: {df_evolucao.count()}")
df_evolucao.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD_PATH}/evolucao_proficiencia/")
print("✓ evolucao_proficiencia gravado na Gold")


# ============================================================
# 3. RANKING MUNICÍPIOS
#    Ranking de municípios por taxa de alfabetização dentro de cada UF e ano
# ============================================================
print("\n" + "="*60)
print("3/5 — GOLD: ranking_municipios")
print("="*60)

# Juntar avaliação de município com meta para ter sigla_uf
# O campo id_municipio tem os 2 primeiros dígitos = código UF
df_mun = df_avaliacao_municipio.withColumn(
    "co_uf", col("id_municipio").cast("string").substr(1, 2).cast(IntegerType())
)

# Mapear código UF para sigla
mapa_uf = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL",
    28: "SE", 29: "BA", 31: "MG", 32: "ES", 33: "RJ", 35: "SP",
    41: "PR", 42: "SC", 43: "RS", 50: "MS", 51: "MT", 52: "GO", 53: "DF"
}

# Criar expressão when encadeada para mapear
from functools import reduce
from pyspark.sql import Column

expr_uf = col("co_uf").cast("string")
for cod, sigla in mapa_uf.items():
    df_mun = df_mun.withColumn("sigla_uf",
        when(col("co_uf") == cod, lit(sigla)).otherwise(
            col("sigla_uf") if "sigla_uf" in df_mun.columns else lit(None)
        )
    )

# Filtrar só quem tem taxa
df_mun_valido = df_mun.filter(col("taxa_alfabetizacao").isNotNull())

# Ranking por UF e ano
window_ranking = Window.partitionBy("ano", "sigla_uf", "rede").orderBy(col("taxa_alfabetizacao").desc())

df_ranking = df_mun_valido.withColumn("ranking", rank().over(window_ranking))
df_ranking = df_ranking.withColumn("percentil", spark_round(percent_rank().over(window_ranking) * 100, 2))
df_ranking = df_ranking.withColumn("_timestamp_gold", current_timestamp())

# Selecionar colunas relevantes
df_ranking = df_ranking.select(
    "ano", "sigla_uf", "id_municipio", "serie", "rede",
    "taxa_alfabetizacao", "media_portugues",
    "ranking", "percentil", "_timestamp_gold"
)

print(f"Registros: {df_ranking.count()}")
df_ranking.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD_PATH}/ranking_municipios/")
print("✓ ranking_municipios gravado na Gold")


# ============================================================
# 4. INDICADORES ALUNOS (do streaming)
#    Agregação dos microdados por UF e município
# ============================================================
print("\n" + "="*60)
print("4/5 — GOLD: indicadores_alunos")
print("="*60)

if tem_alunos:
    # Agregar por UF
    df_ind_uf = df_alunos.filter(col("in_presenca_lp") == 1).groupBy(
        "nu_ano_avaliacao", "sg_uf", "tp_dependencia"
    ).agg(
        count("*").alias("total_alunos_avaliados"),
        spark_sum("in_alfabetizado").alias("total_alfabetizados"),
        avg("vl_proficiencia_lp").alias("media_proficiencia"),
        spark_min("vl_proficiencia_lp").alias("min_proficiencia"),
        spark_max("vl_proficiencia_lp").alias("max_proficiencia"),
    )

    # Calcular taxa
    df_ind_uf = df_ind_uf.withColumn(
        "taxa_alfabetizacao",
        spark_round(col("total_alfabetizados") / col("total_alunos_avaliados") * 100, 2)
    ).withColumn(
        "media_proficiencia", spark_round(col("media_proficiencia"), 2)
    ).withColumn("_timestamp_gold", current_timestamp())

    print(f"Registros: {df_ind_uf.count()}")
    df_ind_uf.write.mode("overwrite").partitionBy("nu_ano_avaliacao").parquet(f"{GOLD_PATH}/indicadores_alunos/")
    print("OK indicadores_alunos gravado na Gold")

    # Agregar por Municipio
    df_ind_mun = df_alunos.filter(col("in_presenca_lp") == 1).groupBy(
        "nu_ano_avaliacao", "sg_uf", "co_municipio", "no_municipio", "tp_dependencia"
    ).agg(
        count("*").alias("total_alunos_avaliados"),
        spark_sum("in_alfabetizado").alias("total_alfabetizados"),
        avg("vl_proficiencia_lp").alias("media_proficiencia"),
    )

    df_ind_mun = df_ind_mun.withColumn(
        "taxa_alfabetizacao",
        spark_round(col("total_alfabetizados") / col("total_alunos_avaliados") * 100, 2)
    ).withColumn(
        "media_proficiencia", spark_round(col("media_proficiencia"), 2)
    ).withColumn("_timestamp_gold", current_timestamp())

    df_ind_mun.write.mode("overwrite").partitionBy("nu_ano_avaliacao").parquet(f"{GOLD_PATH}/indicadores_alunos_municipio/")
    print("OK indicadores_alunos_municipio gravado na Gold")
else:
    print("⚠ Pulando — dados de alunos (streaming) não disponíveis")


# ============================================================
# 5. FEATURES ML
#    Dataset limpo e pronto para treinamento de modelos preditivos
# ============================================================
print("\n" + "="*60)
print("5/5 — GOLD: features_ml")
print("="*60)

# Usar meta_municipio como base (mais granular)
df_feat = df_meta_municipio.filter(
    col("taxa_alfabetizacao").isNotNull() &
    col("meta_alfabetizacao_2030").isNotNull() &
    col("id_municipio").isNotNull()
)

df_feat = df_feat.withColumn(
    "gap_meta_2030",
    spark_round(col("meta_alfabetizacao_2030") - col("taxa_alfabetizacao"), 2)
).withColumn(
    "percentual_atingimento",
    spark_round(col("taxa_alfabetizacao") / col("meta_alfabetizacao_2030") * 100, 2)
).withColumn(
    "distancia_meta_2026",
    when(col("meta_alfabetizacao_2026").isNotNull(),
         spark_round(col("meta_alfabetizacao_2026") - col("taxa_alfabetizacao"), 2))
).withColumn(
    "ja_atingiu_meta",
    when(col("taxa_alfabetizacao") >= col("meta_alfabetizacao_2030"), lit(1)).otherwise(lit(0))
)

# Selecionar features finais
df_feat = df_feat.select(
    "ano",
    "id_municipio",
    "rede",
    "taxa_alfabetizacao",
    "meta_alfabetizacao_2024",
    "meta_alfabetizacao_2025",
    "meta_alfabetizacao_2026",
    "meta_alfabetizacao_2027",
    "meta_alfabetizacao_2028",
    "meta_alfabetizacao_2029",
    "meta_alfabetizacao_2030",
    "nivel_alfabetizacao",
    "percentual_participacao",
    "gap_meta_2030",
    "percentual_atingimento",
    "distancia_meta_2026",
    "ja_atingiu_meta",
).withColumn("_timestamp_gold", current_timestamp())

print(f"Registros: {df_feat.count()}")
df_feat.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD_PATH}/features_ml/")
print("✓ features_ml gravado na Gold")


# ============================================================
# 6. DESIGUALDADE REGIONAL
#    Compara regiões (Norte/Nordeste vs Sul/Sudeste/Centro-Oeste)
# ============================================================
print("\n" + "="*60)
print("6/8 — GOLD: desigualdade_regional")
print("="*60)

# Mapear UF para região
REGIOES = {
    "AC": "Norte", "AM": "Norte", "AP": "Norte", "PA": "Norte",
    "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MS": "Centro-Oeste", "MT": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}

# Criar expressão para mapear sigla_uf → região
df_uf_regiao = df_meta_uf.filter(col("taxa_alfabetizacao").isNotNull())

expr_regiao = lit(None)
for sigla, regiao in REGIOES.items():
    expr_regiao = when(col("sigla_uf") == sigla, lit(regiao)).otherwise(expr_regiao)

df_uf_regiao = df_uf_regiao.withColumn("regiao", expr_regiao)

# Agregar por região e ano
df_desigualdade = df_uf_regiao.groupBy("ano", "regiao", "rede").agg(
    spark_round(avg("taxa_alfabetizacao"), 2).alias("media_taxa_alfabetizacao"),
    spark_round(avg("meta_alfabetizacao_2030"), 2).alias("media_meta_2030"),
    spark_min("taxa_alfabetizacao").alias("min_taxa"),
    spark_max("taxa_alfabetizacao").alias("max_taxa"),
    count("*").alias("total_ufs"),
)

# Calcular gap médio da região
df_desigualdade = df_desigualdade.withColumn(
    "gap_medio_meta_2030",
    spark_round(col("media_meta_2030") - col("media_taxa_alfabetizacao"), 2)
).withColumn(
    "amplitude",
    spark_round(col("max_taxa") - col("min_taxa"), 2)
).withColumn("_timestamp_gold", current_timestamp())

print(f"Registros: {df_desigualdade.count()}")
df_desigualdade.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD_PATH}/desigualdade_regional/")
print("✓ desigualdade_regional gravado na Gold")


# ============================================================
# 7. MUNICÍPIOS CRÍTICOS
#    Top municípios com maior gap para a meta 2030 (situação mais grave)
# ============================================================
print("\n" + "="*60)
print("7/8 — GOLD: municipios_criticos")
print("="*60)

df_criticos = df_meta_municipio.filter(
    col("taxa_alfabetizacao").isNotNull() &
    col("meta_alfabetizacao_2030").isNotNull()
)

df_criticos = df_criticos.withColumn(
    "gap_meta_2030",
    spark_round(col("meta_alfabetizacao_2030") - col("taxa_alfabetizacao"), 2)
).withColumn(
    "percentual_atingimento",
    spark_round(col("taxa_alfabetizacao") / col("meta_alfabetizacao_2030") * 100, 2)
)

# Extrair sigla_uf a partir do id_municipio (2 primeiros dígitos = código UF)
df_criticos = df_criticos.withColumn(
    "co_uf", col("id_municipio").cast("string").substr(1, 2).cast(IntegerType())
)

expr_sigla = lit(None)
for cod, sigla in mapa_uf.items():
    expr_sigla = when(col("co_uf") == cod, lit(sigla)).otherwise(expr_sigla)
df_criticos = df_criticos.withColumn("sigla_uf", expr_sigla)

# Adicionar região
expr_regiao2 = lit(None)
for sigla, regiao in REGIOES.items():
    expr_regiao2 = when(col("sigla_uf") == sigla, lit(regiao)).otherwise(expr_regiao2)
df_criticos = df_criticos.withColumn("regiao", expr_regiao2)

# Classificar nível de criticidade
df_criticos = df_criticos.withColumn("nivel_criticidade",
    when(col("gap_meta_2030") >= 60, "Crítico")
    .when(col("gap_meta_2030") >= 40, "Muito Alto")
    .when(col("gap_meta_2030") >= 25, "Alto")
    .when(col("gap_meta_2030") >= 10, "Moderado")
    .otherwise("Baixo")
)

df_criticos = df_criticos.select(
    "ano", "id_municipio", "sigla_uf", "regiao", "rede",
    "taxa_alfabetizacao", "meta_alfabetizacao_2030",
    "gap_meta_2030", "percentual_atingimento", "nivel_criticidade",
    "percentual_participacao"
).withColumn("_timestamp_gold", current_timestamp())

# Ordenar por gap (mais críticos primeiro)
df_criticos = df_criticos.orderBy(col("gap_meta_2030").desc())

print(f"Registros: {df_criticos.count()}")
df_criticos.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD_PATH}/municipios_criticos/")
print("✓ municipios_criticos gravado na Gold")


# ============================================================
# 8. TENDÊNCIA TEMPORAL
#    Variação ano-a-ano e projeção de atingimento da meta 2030
# ============================================================
print("\n" + "="*60)
print("8/8 — GOLD: tendencia_temporal")
print("="*60)

from pyspark.sql.functions import lag

# Usar meta_uf para tendência (dados mais completos)
df_tend = df_meta_uf.filter(col("taxa_alfabetizacao").isNotNull())
df_tend = df_tend.select("ano", "sigla_uf", "rede", "taxa_alfabetizacao", "meta_alfabetizacao_2030")

# Calcular variação ano-a-ano
window_tend = Window.partitionBy("sigla_uf", "rede").orderBy("ano")
df_tend = df_tend.withColumn("taxa_ano_anterior", lag("taxa_alfabetizacao", 1).over(window_tend))
df_tend = df_tend.withColumn(
    "variacao_anual",
    when(col("taxa_ano_anterior").isNotNull(),
         spark_round(col("taxa_alfabetizacao") - col("taxa_ano_anterior"), 2))
)

# Projetar: no ritmo atual, quantos anos faltam pra atingir meta 2030?
df_tend = df_tend.withColumn(
    "anos_para_meta",
    when(
        (col("variacao_anual").isNotNull()) & (col("variacao_anual") > 0) & (col("meta_alfabetizacao_2030").isNotNull()),
        spark_round((col("meta_alfabetizacao_2030") - col("taxa_alfabetizacao")) / col("variacao_anual"), 1)
    )
).withColumn(
    "ano_projetado_atingimento",
    when(col("anos_para_meta").isNotNull() & (col("anos_para_meta") > 0),
         (col("ano") + col("anos_para_meta")).cast(IntegerType()))
).withColumn(
    "vai_atingir_2030",
    when(col("ano_projetado_atingimento").isNull(), lit("Sem dados"))
    .when(col("taxa_alfabetizacao") >= col("meta_alfabetizacao_2030"), lit("Já atingiu"))
    .when(col("ano_projetado_atingimento") <= 2030, lit("Sim"))
    .otherwise(lit("Não no ritmo atual"))
).withColumn("_timestamp_gold", current_timestamp())

# Remover coluna auxiliar
df_tend = df_tend.drop("taxa_ano_anterior")

print(f"Registros: {df_tend.count()}")
df_tend.write.mode("overwrite").partitionBy("ano").parquet(f"{GOLD_PATH}/tendencia_temporal/")
print("✓ tendencia_temporal gravado na Gold")


# ============================================================
# FINALIZAÇÃO
# ============================================================
print("\n" + "="*60)
print("TRANSFORMAÇÃO SILVER → GOLD COMPLETA!")
print("="*60)
print(f"\nDados gravados em: s3://{BUCKET}/Gold/")
print("Tabelas criadas:")
print("  - Gold/panorama_alfabetizacao/")
print("  - Gold/evolucao_proficiencia/")
print("  - Gold/ranking_municipios/")
print("  - Gold/indicadores_alunos/")
print("  - Gold/indicadores_alunos_municipio/")
print("  - Gold/features_ml/")
print("  - Gold/desigualdade_regional/")
print("  - Gold/municipios_criticos/")
print("  - Gold/tendencia_temporal/")

job.commit()
