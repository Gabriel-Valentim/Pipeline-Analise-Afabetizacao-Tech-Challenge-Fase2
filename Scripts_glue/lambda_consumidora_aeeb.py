import boto3
import base64
import os
import json
from datetime import datetime
import io
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

s3 = boto3.client('s3')
BUCKET = os.environ.get('BUCKET_NAME', 'tech-challange-fase2')


SCHEMA_ATHENA = pa.schema([
    ('nu_ano_avaliacao', pa.int32()),
    ('co_uf', pa.int32()),
    ('sg_uf', pa.string()),
    ('id_aluno', pa.int32()),
    ('tp_serie', pa.int32()),
    ('id_escola', pa.int32()),
    ('tp_dependencia', pa.int32()),
    ('co_municipio', pa.int32()),
    ('no_municipio', pa.string()),
    ('in_presenca_lp', pa.int32()),
    ('in_preenchimento_lp', pa.int32()),
    ('co_caderno_lp', pa.int32()),
    ('co_bloco_1', pa.int32()),
    ('tx_resposta_bloco_1', pa.string()),
    ('tx_gabarito_bloco_1', pa.string()),
    ('co_bloco_2', pa.int32()),
    ('tx_resposta_bloco_2', pa.string()),
    ('tx_gabarito_bloco_2', pa.string()),
    ('co_bloco_3', pa.int32()),
    ('tx_resposta_bloco_3', pa.string()),
    ('tx_gabarito_bloco_3', pa.string()),
    ('co_bloco_4', pa.int32()),
    ('tx_resposta_bloco_4', pa.string()),
    ('tx_gabarito_bloco_4', pa.string()),
    ('vl_peso_aluno_lp', pa.float64()),
    ('vl_proficiencia_lp', pa.float64()),
    ('in_alfabetizado', pa.int32()),
    ('_timestamp_ingestao', pa.timestamp('ms')),
    ('_fonte', pa.string()),
    ('_arquivo_original', pa.string()),
    ('data_ingestao', pa.string())
])


def to_int(v):
    if v is None or str(v).strip() == '' or str(v).lower() == 'nan': return None
    try: return int(float(v))
    except: return None

def to_float(v):
    if v is None or str(v).strip() == '' or str(v).lower() == 'nan': return None
    try: return float(v)
    except: return None

def to_str(v):
    if v is None or str(v).strip() == '' or str(v).lower() == 'nan' or v == 'None': return None
    return str(v)

def lambda_handler(event, context):
    agora = datetime.utcnow()
    dia = agora.strftime('%Y-%m-%d')
    
    registros_validos = []


    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            r = json.loads(payload)
            
            novo_registro = {
                'nu_ano_avaliacao': to_int(r.get('nu_ano_avaliacao')),
                'co_uf': to_int(r.get('co_uf')),
                'sg_uf': to_str(r.get('sg_uf')),
                'id_aluno': to_int(r.get('id_aluno')),
                'tp_serie': to_int(r.get('tp_serie')),
                'id_escola': to_int(r.get('id_escola')),
                'tp_dependencia': to_int(r.get('tp_dependencia')),
                'co_municipio': to_int(r.get('co_municipio')),
                'no_municipio': to_str(r.get('no_municipio')),
                'in_presenca_lp': to_int(r.get('in_presenca_lp')),
                'in_preenchimento_lp': to_int(r.get('in_preenchimento_lp')),
                'co_caderno_lp': to_int(r.get('co_caderno_lp')),
                'co_bloco_1': to_int(r.get('co_bloco_1')),
                'tx_resposta_bloco_1': to_str(r.get('tx_resposta_bloco_1')),
                'tx_gabarito_bloco_1': to_str(r.get('tx_gabarito_bloco_1')),
                'co_bloco_2': to_int(r.get('co_bloco_2')),
                'tx_resposta_bloco_2': to_str(r.get('tx_resposta_bloco_2')),
                'tx_gabarito_bloco_2': to_str(r.get('tx_gabarito_bloco_2')),
                'co_bloco_3': to_int(r.get('co_bloco_3')),
                'tx_resposta_bloco_3': to_str(r.get('tx_resposta_bloco_3')),
                'tx_gabarito_bloco_3': to_str(r.get('tx_gabarito_bloco_3')),
                'co_bloco_4': to_int(r.get('co_bloco_4')),
                'tx_resposta_bloco_4': to_str(r.get('tx_resposta_bloco_4')),
                'tx_gabarito_bloco_4': to_str(r.get('tx_gabarito_bloco_4')),
                'vl_peso_aluno_lp': to_float(r.get('vl_peso_aluno_lp')),
                'vl_proficiencia_lp': to_float(r.get('vl_proficiencia_lp')),
                'in_alfabetizado': to_int(r.get('in_alfabetizado')),
                '_timestamp_ingestao': agora, # PyArrow aceita objetos datetime puros
                '_fonte': 'kinesis_streaming',
                '_arquivo_original': 'stream_kinesis',
                'data_ingestao': dia
            }
            registros_validos.append(novo_registro)
        except Exception as e:
            print(f"Erro ao tratar JSON do Kinesis: {e}")

    if not registros_validos:
        return {'statusCode': 200, 'body': 'Nenhum registro no lote.'}


    table = pa.Table.from_pylist(registros_validos, schema=SCHEMA_ATHENA)
    
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)


    id_unico = uuid.uuid4().hex[:8]
    key = f"Bronze/Ts_aluno/data_ingestao={dia}/{int(agora.timestamp())}_streaming_{id_unico}.parquet"
    
    s3.put_object(
        Bucket=BUCKET, 
        Key=key, 
        Body=buffer.getvalue(), 
        ContentType='application/x-parquet'
    )
    
    qtd = len(registros_validos)
    print(f"Sucesso! {qtd} registros gravados rapidamente via PyArrow nativo.")
    return {'statusCode': 200, 'body': f"Processados {qtd} alunos."}