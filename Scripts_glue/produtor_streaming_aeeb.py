import boto3
import json
import random
import time
import argparse
from datetime import datetime

# ============================================================
# CONFIGURAÇÃO
# ============================================================
STREAM_NAME = 'alfabetizacao-events'
REGION = 'us-east-1'
kinesis = boto3.client('kinesis', region_name=REGION)

# Dados de Referência (Omitindo as listas longas para manter o código limpo, mas a lógica é a mesma)
UFS = [{"CO_UF": 11, "SG_UF": "RO"}, {"CO_UF": 12, "SG_UF": "AC"}, {"CO_UF": 13, "SG_UF": "AM"}, {"CO_UF": 14, "SG_UF": "RR"}, {"CO_UF": 15, "SG_UF": "PA"}, {"CO_UF": 16, "SG_UF": "AP"}, {"CO_UF": 17, "SG_UF": "TO"}, {"CO_UF": 21, "SG_UF": "MA"}, {"CO_UF": 22, "SG_UF": "PI"}, {"CO_UF": 23, "SG_UF": "CE"}, {"CO_UF": 24, "SG_UF": "RN"}, {"CO_UF": 25, "SG_UF": "PB"}, {"CO_UF": 26, "SG_UF": "PE"}, {"CO_UF": 27, "SG_UF": "AL"}, {"CO_UF": 28, "SG_UF": "SE"}, {"CO_UF": 29, "SG_UF": "BA"}, {"CO_UF": 31, "SG_UF": "MG"}, {"CO_UF": 32, "SG_UF": "ES"}, {"CO_UF": 33, "SG_UF": "RJ"}, {"CO_UF": 35, "SG_UF": "SP"}, {"CO_UF": 41, "SG_UF": "PR"}, {"CO_UF": 42, "SG_UF": "SC"}, {"CO_UF": 43, "SG_UF": "RS"}, {"CO_UF": 50, "SG_UF": "MS"}, {"CO_UF": 51, "SG_UF": "MT"}, {"CO_UF": 52, "SG_UF": "GO"}, {"CO_UF": 53, "SG_UF": "DF"}]
MUNICIPIOS = {"SP": [("3550308", "Sao Paulo"), ("3509502", "Campinas")], "MT": [("5103403", "Cuiaba"), ("5107776", "Santa Terezinha")]} # Reduzido para focar na lógica
LETRAS_RESPOSTA = ['A', 'B', 'C', 'D', 'E']

def gerar_respostas(gabarito):
    resposta = ""
    for g in gabarito:
        if g == 'X': resposta += '.'
        elif random.random() < 0.65: resposta += g
        else:
            opcao = random.random()
            if opcao < 0.1: resposta += '.'
            elif opcao < 0.15: resposta += '*'
            else: resposta += random.choice([l for l in LETRAS_RESPOSTA if l != g])
    return resposta

def gerar_gabarito():
    gabarito = ""
    for _ in range(9):
        gabarito += 'X' if random.random() < 0.03 else random.choice(LETRAS_RESPOSTA)
    return gabarito

def gerar_evento_ts_aluno():
    uf_info = random.choice(UFS)
    co_uf, sg_uf = uf_info["CO_UF"], uf_info["SG_UF"]
    if sg_uf in MUNICIPIOS:
        mun = random.choice(MUNICIPIOS[sg_uf])
        co_municipio, no_municipio = int(mun[0]), mun[1]
    else:
        co_municipio, no_municipio = co_uf * 100000 + random.randint(100, 999), f"Municipio {sg_uf}"

    in_presenca_lp = 1 if random.random() < 0.85 else 0
    in_preenchimento_lp = in_presenca_lp

    if in_presenca_lp == 1:
        co_caderno_lp = random.randint(1, 24)
        gab1, gab2, gab3, gab4 = [gerar_gabarito() for _ in range(4)]
        
        co_bloco_1, tx_resposta_bloco_1, tx_gabarito_bloco_1 = random.randint(1, 30), gerar_respostas(gab1), gab1
        co_bloco_2, tx_resposta_bloco_2, tx_gabarito_bloco_2 = random.randint(1, 30), gerar_respostas(gab2), gab2
        co_bloco_3, tx_resposta_bloco_3, tx_gabarito_bloco_3 = random.randint(1, 30), gerar_respostas(gab3), gab3

        if random.random() < 0.8:
            co_bloco_4, tx_resposta_bloco_4, tx_gabarito_bloco_4 = random.randint(1, 30), gerar_respostas(gab4), gab4
        else:
            co_bloco_4 = tx_resposta_bloco_4 = tx_gabarito_bloco_4 = None

        vl_peso_aluno_lp = max(0.1, round(random.gauss(1.2, 0.4), 6))
        vl_proficiencia_lp = max(580.0, min(920.0, round(random.gauss(740, 40), 4)))
        in_alfabetizado = 1 if vl_proficiencia_lp >= 743 else 0
    else:
        co_caderno_lp = co_bloco_1 = tx_resposta_bloco_1 = tx_gabarito_bloco_1 = None
        co_bloco_2 = tx_resposta_bloco_2 = tx_gabarito_bloco_2 = None
        co_bloco_3 = tx_resposta_bloco_3 = tx_gabarito_bloco_3 = None
        co_bloco_4 = tx_resposta_bloco_4 = tx_gabarito_bloco_4 = None
        vl_peso_aluno_lp = vl_proficiencia_lp = in_alfabetizado = None

    # CHAVES EM MINÚSCULO PARA COMBINAR COM O CSV
    evento = {
        "nu_ano_avaliacao": 2025,
        "co_uf": co_uf,
        "sg_uf": sg_uf,
        "id_aluno": random.randint(10000000, 99999999),
        "tp_serie": 2,
        "id_escola": random.randint(10000000, 99999999),
        "tp_dependencia": random.choices([1, 2, 3, 4], weights=[3, 20, 60, 17])[0],
        "co_municipio": co_municipio,
        "no_municipio": no_municipio,
        "in_presenca_lp": in_presenca_lp,
        "in_preenchimento_lp": in_preenchimento_lp,
        "co_caderno_lp": co_caderno_lp,
        "co_bloco_1": co_bloco_1,
        "tx_resposta_bloco_1": tx_resposta_bloco_1,
        "tx_gabarito_bloco_1": tx_gabarito_bloco_1,
        "co_bloco_2": co_bloco_2,
        "tx_resposta_bloco_2": tx_resposta_bloco_2,
        "tx_gabarito_bloco_2": tx_gabarito_bloco_2,
        "co_bloco_3": co_bloco_3,
        "tx_resposta_bloco_3": tx_resposta_bloco_3,
        "tx_gabarito_bloco_3": tx_gabarito_bloco_3,
        "co_bloco_4": co_bloco_4,
        "tx_resposta_bloco_4": tx_resposta_bloco_4,
        "tx_gabarito_bloco_4": tx_gabarito_bloco_4,
        "vl_peso_aluno_lp": vl_peso_aluno_lp,
        "vl_proficiencia_lp": vl_proficiencia_lp,
        "in_alfabetizado": in_alfabetizado,
    }
    return evento

def publicar_evento(evento):
    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(evento, ensure_ascii=False),
        PartitionKey=evento["sg_uf"] # Atualizado para minúsculo
    )
    return response['SequenceNumber']

def publicar_lote(eventos):
    records = [{'Data': json.dumps(e, ensure_ascii=False).encode('utf-8'), 'PartitionKey': e["sg_uf"]} for e in eventos]
    response = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
    return response['FailedRecordCount']

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--quantidade', type=int, default=100)
    parser.add_argument('--intervalo', type=float, default=0.5)
    parser.add_argument('--lote', action='store_true')
    args = parser.parse_args()

    print(f"Iniciando envio para {STREAM_NAME}...")
    total_enviados = total_presentes = total_alfabetizados = 0

    if args.lote:
        for i in range(0, args.quantidade, 50):
            tam = min(50, args.quantidade - i)
            eventos = [gerar_evento_ts_aluno() for _ in range(tam)]
            falhas = publicar_lote(eventos)
            total_enviados += tam - falhas
            total_presentes += sum(1 for e in eventos if e["in_presenca_lp"] == 1)
            total_alfabetizados += sum(1 for e in eventos if e.get("in_alfabetizado") == 1)
            print(f"Lote {i//50+1}: {tam} enviados.")
            time.sleep(args.intervalo)
    else:
        for i in range(args.quantidade):
            e = gerar_evento_ts_aluno()
            publicar_evento(e)
            total_enviados += 1
            if e["in_presenca_lp"] == 1: total_presentes += 1
            if e.get("in_alfabetizado") == 1: total_alfabetizados += 1
            print(f"[{i+1}/{args.quantidade}] {e['sg_uf']} | Aluno {e['id_aluno']} enviado.")
            time.sleep(args.intervalo)

if __name__ == "__main__":
    main()