"""
Lambda Consumidora — Processa registros TS_ALUNO do Kinesis e grava na Bronze (S3).

Trigger: Kinesis Data Stream 'alfabetizacao-events'
Destino de falha: SQS 'alfabetizacao-dlq'

O que faz:
  1. Recebe batch de records do Kinesis (cada record = 1 aluno)
  2. Valida os campos obrigatórios do schema TS_ALUNO
  3. Se válido: grava como PARQUET no S3 (camada Bronze), particionado por data_ingestao
  4. Se inválido: envia para a dead-letter queue (SQS) para investigação

Caminho no S3:
  s3://BUCKET/Bronze/Ts_aluno/data_ingestao=AAAA-MM-DD/{timestamp}_{id_aluno}.parquet

Configuração necessária:
  - Variáveis de ambiente na Lambda:
      BUCKET_NAME = tech-challange-fase2
      DLQ_URL = https://sqs.us-east-1.amazonaws.com/SEU_ACCOUNT/alfabetizacao-dlq
  - IAM Role com permissões: s3:PutObject, sqs:SendMessage, kinesis:GetRecords
  - Trigger: Kinesis stream 'alfabetizacao-events', batch size 100, starting position LATEST
  - Layer: pandas + pyarrow (ou use AWSSDKPandas-Python312)
"""

import boto3
import base64
import os
import json
from datetime import datetime
import pandas as pd
import numpy as np
import io

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

BUCKET = os.environ.get('BUCKET_NAME', 'tech-challange-fase2')
DLQ_URL = os.environ.get('DLQ_URL', '')

# Colunas na ordem final do Parquet
COLUNAS_ORDENADAS = [
    'nu_ano_avaliacao', 'co_uf', 'sg_uf', 'id_aluno', 'tp_serie',
    'id_escola', 'tp_dependencia', 'co_municipio', 'no_municipio',
    'in_presenca_lp', 'in_preenchimento_lp', 'co_caderno_lp',
    'co_bloco_1', 'tx_resposta_bloco_1', 'tx_gabarito_bloco_1',
    'co_bloco_2', 'tx_resposta_bloco_2', 'tx_gabarito_bloco_2',
    'co_bloco_3', 'tx_resposta_bloco_3', 'tx_gabarito_bloco_3',
    'co_bloco_4', 'tx_resposta_bloco_4', 'tx_gabarito_bloco_4',
    'vl_peso_aluno_lp', 'vl_proficiencia_lp', 'in_alfabetizado',
    '_timestamp_ingestao', '_fonte', '_arquivo_original', 'data_ingestao'
]

# Tipos — usando Int64 (nullable) para inteiros que podem ser None
# e float64 para numéricos que podem ser None
DTYPE_MAP = {
    'nu_ano_avaliacao': 'Int64',
    'co_uf': 'Int64',
    'sg_uf': 'string',
    'id_aluno': 'Int64',
    'tp_serie': 'Int64',
    'id_escola': 'Int64',
    'tp_dependencia': 'Int64',
    'co_municipio': 'Int64',
    'no_municipio': 'string',
    'in_presenca_lp': 'Int64',
    'in_preenchimento_lp': 'Int64',
    'co_caderno_lp': 'Int64',
    'co_bloco_1': 'Int64',
    'tx_resposta_bloco_1': 'string',
    'tx_gabarito_bloco_1': 'string',
    'co_bloco_2': 'Int64',
    'tx_resposta_bloco_2': 'string',
    'tx_gabarito_bloco_2': 'string',
    'co_bloco_3': 'Int64',
    'tx_resposta_bloco_3': 'string',
    'tx_gabarito_bloco_3': 'string',
    'co_bloco_4': 'Int64',
    'tx_resposta_bloco_4': 'string',
    'tx_gabarito_bloco_4': 'string',
    'vl_peso_aluno_lp': 'float64',
    'vl_proficiencia_lp': 'float64',
    'in_alfabetizado': 'Int64',
    '_timestamp_ingestao': 'string',
    '_fonte': 'string',
    '_arquivo_original': 'string',
    'data_ingestao': 'string',
}


def normalizar_chaves(registro):
    """Converte chaves do registro para minúsculo (Kinesis envia maiúsculo)."""
    return {k.lower(): v for k, v in registro.items()}


def gravar_no_s3(registro, event_id):
    """Grava registro do aluno como Parquet no S3."""
    agora = datetime.utcnow()
    dia = agora.strftime('%Y-%m-%d')

    # Normalizar chaves para minúsculo
    registro = normalizar_chaves(registro)

    # Adicionar metadados
    registro['_timestamp_ingestao'] = agora.strftime('%Y-%m-%d %H:%M:%S.') + f"{agora.microsecond // 1000:03d}"
    registro['_fonte'] = 'base_dos_dados'
    registro['_arquivo_original'] = 'TS_ALUNO.csv'
    registro['data_ingestao'] = dia

    # Criar DataFrame com uma linha
    df = pd.DataFrame([registro])

    # Garantir que todas as colunas existam
    for col in COLUNAS_ORDENADAS:
        if col not in df.columns:
            df[col] = None

    # Reordenar e aplicar tipos (Int64 aceita None/NA)
    df = df[COLUNAS_ORDENADAS]
    for col, dtype in DTYPE_MAP.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)

    # Gravar como Parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    id_aluno = registro.get('id_aluno', 'unknown')
    key = f"Bronze/Ts_aluno/data_ingestao={dia}/{int(agora.timestamp())}_{id_aluno}.parquet"

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType='application/x-parquet'
    )
    return key


def enviar_para_dlq(registro, motivo, event_id):
    """Envia registro inválido para a dead-letter queue."""
    if not DLQ_URL:
        print(f"DLQ não configurada. Rejeitado: {motivo}")
        return

    sqs.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps({
            'registro_original': registro,
            'motivo_rejeicao': motivo,
            'event_id': event_id,
            'timestamp_rejeicao': datetime.utcnow().isoformat() + 'Z'
        }, ensure_ascii=False, default=str)
    )


def lambda_handler(event, context):
    """Handler principal — processa batch de records do Kinesis."""
    total = len(event['Records'])
    processados = 0
    erros = 0

    for record in event['Records']:
        event_id = record.get('eventID', 'unknown')

        try:
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            registro = json.loads(payload)

            gravar_no_s3(registro, event_id)
            processados += 1

        except Exception as e:
            print(f"ERRO [{event_id}]: {str(e)}")
            try:
                enviar_para_dlq(registro if 'registro' in dir() else {'raw': payload}, str(e), event_id)
            except:
                pass
            erros += 1

    print(f"RESUMO: {total} recebidos | {processados} gravados | {erros} erros")

    return {
        'statusCode': 200,
        'body': {
            'total_recebidos': total,
            'processados': processados,
            'erros': erros
        }
    }
