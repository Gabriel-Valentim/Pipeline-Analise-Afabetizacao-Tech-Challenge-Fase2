"""
AWS Glue Job — Ingestão Raw → Bronze
Pipeline de Alfabetização - Tech Challenge Fase 2


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
BUCKET = "tech-challange-fase2"
RAW_PATH = f"s3://tech-challange-fase2/Raw"
BRONZE_PATH = f"s3://tech-challange-fase2/Bronze"


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
    input_path = f"s3://tech-challange-fase2/Raw/{nome_fonte}/{nome_arquivo_csv}"
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
    output_path = f"s3://tech-challange-fase2/Bronze/{nome_fonte}/"
    print(f"Gravando em: {output_path}")
    
    df.write \
        .mode("overwrite") \
        .partitionBy(particao_campo) \
        .parquet(output_path)
    
    print(f"✓ {nome_fonte} ingerido com sucesso!")
    return df.count()



from pyspark.sql.functions import current_timestamp, current_date, lit

def ingerir_para_bronze_ts_aluno(nome_arquivo_csv, nome_fonte, particao_campo="data_ingestao"):
    """
    Lê um CSV da camada raw, adiciona metadados (incluindo data de ingestão) e grava em Parquet na Bronze.
    
    Args:
        nome_arquivo_csv: Nome do arquivo CSV no S3 (sem extensão .csv se a pasta já tem)
        nome_fonte: Nome descritivo da fonte (usado como subpasta na Bronze)
        particao_campo: Campo para particionamento (default: "data_ingestao")
    """
    print(f"\n{'='*60}")
    print(f"Ingerindo: {nome_fonte}")
    print(f"{'='*60}")
    
    # Ler CSV
    input_path = f"s3://tech-challange-fase2/Raw/{nome_fonte}/{nome_arquivo_csv}"
    print(f"Lendo de: {input_path}")
    
    df = spark.read \
        .option("header", True) \
        .option("sep",";") \
        .option("inferSchema", True) \
        .csv(input_path)
    
    print(f"Registros lidos: {df.count()}")
    print(f"Colunas: {df.columns}")
    
    # Adicionar metadados de ingestão e a NOVA COLUNA de data de hoje
    df = df.withColumn("_timestamp_ingestao", current_timestamp()) \
           .withColumn("_fonte", lit("base_dos_dados")) \
           .withColumn("_arquivo_original", lit(nome_arquivo_csv)) \
           .withColumn("data_ingestao", current_date()) # <-- Nova coluna criada aqui
    
    # Gravar em Parquet particionado
    output_path = f"s3://tech-challange-fase2/Bronze/{nome_fonte}/"
    print(f"Gravando em: {output_path} particionado por {particao_campo}")
    
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
    nome_fonte="Meta_brasil"
)


# ============================================================
# 2. META ALFABETIZAÇÃO UF (ESTADOS)
# Colunas: ano, sigla_uf, rede, taxa_alfabetizacao, meta_2024..2030, percentual_participacao
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    nome_fonte="Meta_uf"
)


# ============================================================
# 3. META ALFABETIZAÇÃO MUNICÍPIO
# Colunas: ano, id_municipio, rede, taxa_alfabetizacao, meta_2024..2030, 
#          nivel_alfabetizacao, percentual_participacao
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
    nome_fonte="Meta_municipio"
)


# ============================================================
# 4. AVALIAÇÃO ALFABETIZAÇÃO UF (ESTADOS)
# Colunas: ano, sigla_uf, serie, rede, taxa_alfabetizacao, media_portugues,
#          proporcao_aluno_nivel_0..nivel_8
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_uf.csv",
    nome_fonte="Avaliacao_uf"
)


# ============================================================
# 5. AVALIAÇÃO ALFABETIZAÇÃO MUNICÍPIO
# Colunas: ano, id_municipio, serie, rede, taxa_alfabetizacao, media_portugues,
#          proporcao_aluno_nivel_0..nivel_8
# ============================================================
ingerir_para_bronze(
    nome_arquivo_csv="br_inep_avaliacao_alfabetizacao_municipio.csv",
    nome_fonte="Avaliacao_municipio"
)


# ============================================================
# 6. TABELA TS ALUNOS

# ============================================================
ingerir_para_bronze_ts_aluno(
    nome_arquivo_csv="TS_ALUNO.csv",
    nome_fonte="Ts_aluno",
    particao_campo="data_ingestao"
)


# ============================================================
# FINALIZAÇÃO
# ============================================================
print("\n" + "="*60)
print("INGESTÃO BRONZE COMPLETA!")
print("="*60)
print(f"\nDados gravados em: s3://tech-challange-fase2/Bronze/")
print("Subpastas criadas:")
print("  - Bronze/Meta_brasil/")
print("  - Bronze/Meta_uf/")
print("  - Bronze/Meta_municipio/")
print("  - Bronze/Avaliacao_uf/")
print("  - Bronze/Avaliacao_municipio/")
print("  - Bronze/Ts_alunos/")
job.commit()
