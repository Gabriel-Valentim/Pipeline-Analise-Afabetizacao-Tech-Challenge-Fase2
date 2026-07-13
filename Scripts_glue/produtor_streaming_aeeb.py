"""
Produtor de Eventos Streaming — Microdados AEEB 2025 (TS_ALUNO)
================================================================

Simula a ingestão via streaming de dados de alunos da AEEB 2025.
Cada evento contém TODOS os campos da tabela TS_ALUNO conforme o
Dicionário de Microdados AEEB 2025.

Campos publicados (schema completo TS_ALUNO):
- NU_ANO_AVALIACAO (int, 4): Ano de aplicação da avaliação estadual
- CO_UF (int, 2): Código da Unidade da Federação
- SG_UF (str, 2): Sigla da Unidade da Federação
- ID_ALUNO (int, 8): Identificador do Aluno
- TP_SERIE (int, 1): Ano Escolar (2 = 2º ano EF)
- ID_ESCOLA (int, 8): Máscara do Código da Escola (fictício)
- TP_DEPENDENCIA (int, 1): Dependência Administrativa (1=Federal,2=Estadual,3=Municipal,4=Privada)
- CO_MUNICIPIO (int, 7): Código IBGE do Município da escola
- NO_MUNICIPIO (str, 150): Nome do Município da escola
- IN_PRESENCA_LP (int, 1): Presença na prova de LP (0=Ausente,1=Presente)
- IN_PREENCHIMENTO_LP (int, 1): Preenchimento da prova de LP (0=Não,1=Sim)
- CO_CADERNO_LP (int, 2): Código do Caderno de LP
- CO_BLOCO_1 (int, 2): Código do Bloco posição 1
- TX_RESPOSTA_BLOCO_1 (str, 9): Respostas dos itens objetivos bloco 1
- TX_GABARITO_BLOCO_1 (str, 9): Gabarito dos itens objetivos bloco 1
- CO_BLOCO_2 (int, 2): Código do Bloco posição 2
- TX_RESPOSTA_BLOCO_2 (str, 9): Respostas dos itens objetivos bloco 2
- TX_GABARITO_BLOCO_2 (str, 9): Gabarito dos itens objetivos bloco 2
- CO_BLOCO_3 (int, 2): Código do Bloco posição 3
- TX_RESPOSTA_BLOCO_3 (str, 9): Respostas dos itens objetivos bloco 3
- TX_GABARITO_BLOCO_3 (str, 9): Gabarito dos itens objetivos bloco 3
- CO_BLOCO_4 (int, 2): Código do Bloco posição 4
- TX_RESPOSTA_BLOCO_4 (str, 9): Respostas dos itens objetivos bloco 4
- TX_GABARITO_BLOCO_4 (str, 9): Gabarito dos itens objetivos bloco 4
- VL_PESO_ALUNO_LP (float, 12): Peso do aluno na prova de LP
- VL_PROFICIENCIA_LP (float, 12): Proficiência em LP na escala SAEB
- IN_ALFABETIZADO (int, 1): Alfabetizado (0=Não,1=Sim; corte >= 743)

Como usar:
  python produtor_streaming_aeeb.py --quantidade 100 --intervalo 0.5
  python produtor_streaming_aeeb.py --quantidade 1000 --lote
"""

import boto3
import json
import random
import time
import argparse
import string
from datetime import datetime

# ============================================================
# CONFIGURAÇÃO
# ============================================================
STREAM_NAME = 'alfabetizacao-events'
REGION = 'us-east-1'

kinesis = boto3.client('kinesis', region_name=REGION)

# ============================================================
# DADOS DE REFERÊNCIA
# ============================================================
UFS = [
    {"CO_UF": 11, "SG_UF": "RO"}, {"CO_UF": 12, "SG_UF": "AC"},
    {"CO_UF": 13, "SG_UF": "AM"}, {"CO_UF": 14, "SG_UF": "RR"},
    {"CO_UF": 15, "SG_UF": "PA"}, {"CO_UF": 16, "SG_UF": "AP"},
    {"CO_UF": 17, "SG_UF": "TO"}, {"CO_UF": 21, "SG_UF": "MA"},
    {"CO_UF": 22, "SG_UF": "PI"}, {"CO_UF": 23, "SG_UF": "CE"},
    {"CO_UF": 24, "SG_UF": "RN"}, {"CO_UF": 25, "SG_UF": "PB"},
    {"CO_UF": 26, "SG_UF": "PE"}, {"CO_UF": 27, "SG_UF": "AL"},
    {"CO_UF": 28, "SG_UF": "SE"}, {"CO_UF": 29, "SG_UF": "BA"},
    {"CO_UF": 31, "SG_UF": "MG"}, {"CO_UF": 32, "SG_UF": "ES"},
    {"CO_UF": 33, "SG_UF": "RJ"}, {"CO_UF": 35, "SG_UF": "SP"},
    {"CO_UF": 41, "SG_UF": "PR"}, {"CO_UF": 42, "SG_UF": "SC"},
    {"CO_UF": 43, "SG_UF": "RS"}, {"CO_UF": 50, "SG_UF": "MS"},
    {"CO_UF": 51, "SG_UF": "MT"}, {"CO_UF": 52, "SG_UF": "GO"},
    {"CO_UF": 53, "SG_UF": "DF"},
]

MUNICIPIOS = {
    "SP": [("3550308", "Sao Paulo"), ("3509502", "Campinas"), ("3547809", "Santo Andre")],
    "MG": [("3106200", "Belo Horizonte"), ("3170206", "Uberlandia")],
    "RJ": [("3304557", "Rio de Janeiro"), ("3301702", "Duque de Caxias")],
    "BA": [("2927408", "Salvador"), ("2910800", "Feira de Santana")],
    "CE": [("2304400", "Fortaleza"), ("2307908", "Juazeiro do Norte")],
    "PR": [("4106902", "Curitiba"), ("4113700", "Londrina")],
    "PE": [("2611606", "Recife"), ("2607901", "Jaboatao dos Guararapes")],
    "RS": [("4314902", "Porto Alegre"), ("4303905", "Caxias do Sul")],
    "MA": [("2111300", "Sao Luis"), ("2105302", "Imperatriz")],
    "GO": [("5208707", "Goiania"), ("5201405", "Aparecida de Goiania")],
    "PA": [("1501402", "Belem"), ("1500800", "Ananindeua")],
    "SC": [("4205407", "Florianopolis"), ("4209102", "Joinville")],
    "AM": [("1302603", "Manaus")], "AL": [("2704302", "Maceio")],
    "SE": [("2800308", "Aracaju")], "PI": [("2211001", "Teresina")],
    "RN": [("2408102", "Natal")], "PB": [("2507507", "Joao Pessoa")],
    "MT": [("5103403", "Cuiaba")], "MS": [("5002704", "Campo Grande")],
    "DF": [("5300108", "Brasilia")], "TO": [("1721000", "Palmas")],
    "RO": [("1100205", "Porto Velho")], "AC": [("1200401", "Rio Branco")],
    "AP": [("1600303", "Macapa")], "RR": [("1400100", "Boa Vista")],
}

LETRAS_RESPOSTA = ['A', 'B', 'C', 'D', 'E']


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================
def gerar_respostas(gabarito):
    """Gera vetor de 9 respostas simuladas baseado no gabarito."""
    resposta = ""
    for g in gabarito:
        if g == 'X':
            resposta += '.'
        elif random.random() < 0.65:  # 65% de acerto
            resposta += g
        else:
            # Resposta errada ou em branco
            opcao = random.random()
            if opcao < 0.1:
                resposta += '.'  # em branco
            elif opcao < 0.15:
                resposta += '*'  # marcação múltipla
            else:
                erradas = [l for l in LETRAS_RESPOSTA if l != g]
                resposta += random.choice(erradas)
    return resposta


def gerar_gabarito():
    """Gera vetor de gabarito de 9 itens (pode ter X para anulado)."""
    gabarito = ""
    for _ in range(9):
        if random.random() < 0.03:  # 3% de chance de item anulado
            gabarito += 'X'
        else:
            gabarito += random.choice(LETRAS_RESPOSTA)
    return gabarito


# ============================================================
# GERADOR DO EVENTO TS_ALUNO COMPLETO
# ============================================================
def gerar_evento_ts_aluno():
    """
    Gera um registro completo da tabela TS_ALUNO conforme dicionário AEEB 2025.
    Inclui TODOS os campos da tabela.
    """
    # UF e município
    uf_info = random.choice(UFS)
    co_uf = uf_info["CO_UF"]
    sg_uf = uf_info["SG_UF"]

    if sg_uf in MUNICIPIOS:
        mun = random.choice(MUNICIPIOS[sg_uf])
        co_municipio = int(mun[0])
        no_municipio = mun[1]
    else:
        co_municipio = co_uf * 100000 + random.randint(100, 999)
        no_municipio = f"Municipio {sg_uf}"

    # Presença (85% presentes)
    in_presenca_lp = 1 if random.random() < 0.85 else 0
    in_preenchimento_lp = in_presenca_lp

    # Dados da prova (só se presente e preencheu)
    if in_presenca_lp == 1:
        co_caderno_lp = random.randint(1, 24)

        # Blocos 1 a 4: gabarito e respostas
        gab1 = gerar_gabarito()
        gab2 = gerar_gabarito()
        gab3 = gerar_gabarito()
        gab4 = gerar_gabarito()

        co_bloco_1 = random.randint(1, 30)
        tx_resposta_bloco_1 = gerar_respostas(gab1)
        tx_gabarito_bloco_1 = gab1

        co_bloco_2 = random.randint(1, 30)
        tx_resposta_bloco_2 = gerar_respostas(gab2)
        tx_gabarito_bloco_2 = gab2

        co_bloco_3 = random.randint(1, 30)
        tx_resposta_bloco_3 = gerar_respostas(gab3)
        tx_gabarito_bloco_3 = gab3

        # Bloco 4 pode ser missing (20% de chance)
        if random.random() < 0.8:
            co_bloco_4 = random.randint(1, 30)
            tx_resposta_bloco_4 = gerar_respostas(gab4)
            tx_gabarito_bloco_4 = gab4
        else:
            co_bloco_4 = None
            tx_resposta_bloco_4 = None
            tx_gabarito_bloco_4 = None

        # Peso e proficiência
        vl_peso_aluno_lp = round(random.gauss(1.2, 0.4), 6)
        vl_peso_aluno_lp = max(0.1, vl_peso_aluno_lp)

        vl_proficiencia_lp = round(random.gauss(740, 40), 4)
        vl_proficiencia_lp = max(580.0, min(920.0, vl_proficiencia_lp))

        in_alfabetizado = 1 if vl_proficiencia_lp >= 743 else 0
    else:
        # Aluno ausente: campos da prova ficam nulos
        co_caderno_lp = None
        co_bloco_1 = None
        tx_resposta_bloco_1 = None
        tx_gabarito_bloco_1 = None
        co_bloco_2 = None
        tx_resposta_bloco_2 = None
        tx_gabarito_bloco_2 = None
        co_bloco_3 = None
        tx_resposta_bloco_3 = None
        tx_gabarito_bloco_3 = None
        co_bloco_4 = None
        tx_resposta_bloco_4 = None
        tx_gabarito_bloco_4 = None
        vl_peso_aluno_lp = None
        vl_proficiencia_lp = None
        in_alfabetizado = None

    # Montar registro completo TS_ALUNO
    evento = {
        "NU_ANO_AVALIACAO": 2025,
        "CO_UF": co_uf,
        "SG_UF": sg_uf,
        "ID_ALUNO": random.randint(10000000, 99999999),
        "TP_SERIE": 2,
        "ID_ESCOLA": random.randint(10000000, 99999999),
        "TP_DEPENDENCIA": random.choices([1, 2, 3, 4], weights=[3, 20, 60, 17])[0],
        "CO_MUNICIPIO": co_municipio,
        "NO_MUNICIPIO": no_municipio,
        "IN_PRESENCA_LP": in_presenca_lp,
        "IN_PREENCHIMENTO_LP": in_preenchimento_lp,
        "CO_CADERNO_LP": co_caderno_lp,
        "CO_BLOCO_1": co_bloco_1,
        "TX_RESPOSTA_BLOCO_1": tx_resposta_bloco_1,
        "TX_GABARITO_BLOCO_1": tx_gabarito_bloco_1,
        "CO_BLOCO_2": co_bloco_2,
        "TX_RESPOSTA_BLOCO_2": tx_resposta_bloco_2,
        "TX_GABARITO_BLOCO_2": tx_gabarito_bloco_2,
        "CO_BLOCO_3": co_bloco_3,
        "TX_RESPOSTA_BLOCO_3": tx_resposta_bloco_3,
        "TX_GABARITO_BLOCO_3": tx_gabarito_bloco_3,
        "CO_BLOCO_4": co_bloco_4,
        "TX_RESPOSTA_BLOCO_4": tx_resposta_bloco_4,
        "TX_GABARITO_BLOCO_4": tx_gabarito_bloco_4,
        "VL_PESO_ALUNO_LP": vl_peso_aluno_lp,
        "VL_PROFICIENCIA_LP": vl_proficiencia_lp,
        "IN_ALFABETIZADO": in_alfabetizado,
    }

    return evento


# ============================================================
# PUBLICAÇÃO NO KINESIS
# ============================================================
def publicar_evento(evento):
    """Publica um evento no Kinesis."""
    response = kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(evento, ensure_ascii=False),
        PartitionKey=evento["SG_UF"]
    )
    return response['SequenceNumber']


def publicar_lote(eventos):
    """Publica até 500 eventos em batch no Kinesis."""
    records = [
        {
            'Data': json.dumps(e, ensure_ascii=False).encode('utf-8'),
            'PartitionKey': e["SG_UF"]
        }
        for e in eventos
    ]
    response = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
    return response['FailedRecordCount']


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='Produtor streaming — TS_ALUNO AEEB 2025')
    parser.add_argument('--quantidade', type=int, default=100,
                        help='Número de registros de aluno a enviar (default: 100)')
    parser.add_argument('--intervalo', type=float, default=0.5,
                        help='Intervalo entre envios em segundos (default: 0.5)')
    parser.add_argument('--lote', action='store_true',
                        help='Enviar em lotes de 50 (mais rápido)')
    args = parser.parse_args()

    print("=" * 65)
    print("  STREAMING — Microdados TS_ALUNO (AEEB 2025)")
    print("  Todos os campos do dicionário de microdados")
    print("=" * 65)
    print(f"  Stream:     {STREAM_NAME}")
    print(f"  Região:     {REGION}")
    print(f"  Eventos:    {args.quantidade}")
    print(f"  Modo:       {'Lote (50/vez)' if args.lote else 'Individual'}")
    print(f"  Intervalo:  {args.intervalo}s")
    print("-" * 65)

    total_enviados = 0
    total_presentes = 0
    total_alfabetizados = 0
    inicio = time.time()

    if args.lote:
        for i in range(0, args.quantidade, 50):
            tam = min(50, args.quantidade - i)
            eventos = [gerar_evento_ts_aluno() for _ in range(tam)]
            falhas = publicar_lote(eventos)
            total_enviados += tam - falhas
            total_presentes += sum(1 for e in eventos if e["IN_PRESENCA_LP"] == 1)
            total_alfabetizados += sum(1 for e in eventos if e.get("IN_ALFABETIZADO") == 1)
            print(f"  Lote {i//50+1}: {tam} alunos enviados ({falhas} falhas)")
            time.sleep(args.intervalo)
    else:
        for i in range(args.quantidade):
            evento = gerar_evento_ts_aluno()
            publicar_evento(evento)
            total_enviados += 1
            if evento["IN_PRESENCA_LP"] == 1:
                total_presentes += 1
            if evento.get("IN_ALFABETIZADO") == 1:
                total_alfabetizados += 1

            if (i + 1) % 10 == 0:
                taxa = (total_alfabetizados / total_presentes * 100) if total_presentes else 0
                print(f"  [{i+1}/{args.quantidade}] "
                      f"{evento['SG_UF']} | "
                      f"Escola {evento['ID_ESCOLA']} | "
                      f"Prof: {evento.get('VL_PROFICIENCIA_LP', '-')} | "
                      f"Alfab: {'S' if evento.get('IN_ALFABETIZADO')==1 else 'N'} | "
                      f"Taxa parcial: {taxa:.1f}%")
            time.sleep(args.intervalo)

    duracao = time.time() - inicio
    taxa_final = (total_alfabetizados / total_presentes * 100) if total_presentes else 0

    print("\n" + "=" * 65)
    print("  RESULTADO")
    print("=" * 65)
    print(f"  Enviados:       {total_enviados} registros TS_ALUNO")
    print(f"  Duração:        {duracao:.1f}s")
    print(f"  Presentes:      {total_presentes} ({total_presentes/total_enviados*100:.0f}%)")
    print(f"  Alfabetizados:  {total_alfabetizados} ({taxa_final:.1f}% dos presentes)")
    print("=" * 65)


if __name__ == "__main__":
    main()
