# Pipeline Híbrido para Análise da Alfabetização no Brasil

## Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do país. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados, Distrito Federal e municípios com o objetivo de garantir que todas as crianças brasileiras estejam alfabetizadas até o final do **2º ano do ensino fundamental**.

Em 2023, o INEP realizou a **Pesquisa Alfabetiza Brasil**, definindo o ponto de corte de **743 pontos na escala de proficiência do SAEB** como limiar de alfabetização. Com base nisso, foi criado o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar.

A **meta nacional é que até 2030, todas as crianças brasileiras estejam alfabetizadas ao final do 2º ano do ensino fundamental** (taxa de 80%).

## O Desafio

Este projeto implementa uma **pipeline híbrida de dados (Batch + Streaming)** em ambiente de nuvem AWS, seguindo a **Arquitetura Medalhão** (Bronze → Silver → Gold), capaz de integrar diferentes fontes de dados educacionais relacionadas ao indicador de alfabetização.

## Arquitetura Proposta

### Visão Geral

```
┌─────────────────────────────────────────────────────────────┐
│                      FONTES DE DADOS                         │
│  Base dos Dados (5 CSVs)  ──┐    Microdados AEEB 2025 ──┐  │
│  (Batch - histórico)        │    (Streaming - alunos)    │  │
└─────────────────────────────┼────────────────────────────┼──┘
                              ▼                            ▼
                    ┌──────────────┐            ┌──────────────┐
                    │   S3 (raw)   │            │   Kinesis    │
                    └──────┬───────┘            └──────┬───────┘
                           ▼                           ▼
                    ┌──────────────┐            ┌──────────────┐
                    │  Glue Job    │            │    Lambda     │
                    │  (Ingestão)  │            │ (Consumidora) │
                    └──────┬───────┘            └──────┬───────┘
                           ▼                           ▼
              ┌────────────────────────────────────────────────┐
              │            S3 — BRONZE (Parquet)                │
              │  Dados brutos, sem transformação                │
              └────────────────────────┬───────────────────────┘
                                       ▼
              ┌────────────────────────────────────────────────┐
              │     Glue Job (Limpeza + Validação + Dedup)      │
              └────────────────────────┬───────────────────────┘
                                       ▼
              ┌────────────────────────────────────────────────┐
              │            S3 — SILVER (Parquet)                │
              │  Dados limpos, tipados, integrados              │
              └────────────────────────┬───────────────────────┘
                                       ▼
              ┌────────────────────────────────────────────────┐
              │     Glue Job (Agregações + Joins + KPIs)        │
              └────────────────────────┬───────────────────────┘
                                       ▼
              ┌────────────────────────────────────────────────┐
              │             S3 — GOLD (Parquet)                 │
              │  8 tabelas analíticas prontas para consumo      │
              └────────────────────────┬───────────────────────┘
                                       ▼
              ┌────────────┐  ┌────────────┐  ┌────────────┐
              │   Athena   │  │ QuickSight │  │ SageMaker  │
              │  (Queries) │  │(Dashboards)│  │   (ML)     │
              └────────────┘  └────────────┘  └────────────┘
```

### Componentes AWS Utilizados

| Serviço | Função | Justificativa |
|---------|--------|---------------|
| S3 | Data Lake (Bronze/Silver/Gold) | Custo baixo, escalável, suporta Parquet |
| AWS Glue | ETL (Spark) — transformações batch | Serverless, paga por uso, suporta PySpark |
| Kinesis Data Streams | Buffer de streaming | Baixa latência, integração nativa com Lambda |
| Lambda | Consumidora de streaming | Serverless, escala automática, custo mínimo |
| EventBridge | Agendamento do produtor | Trigger periódico sem infraestrutura |
| Athena | Consultas SQL sobre o Data Lake | Pay-per-query, sem servidor, integra com Glue Catalog |
| CloudWatch | Monitoramento e alertas | Logs centralizados, métricas customizadas |
| SQS | Dead-letter queue | Eventos com erro ficam preservados para análise |
| Glue Crawler | Catalogação automática | Detecta schema dos Parquets e cria tabelas |

## Fontes de Dados

### Batch (ingestão periódica)
Dados obtidos da plataforma **Base dos Dados** (Indicador Criança Alfabetizada):

| Fonte | Granularidade | Campos principais |
|-------|---------------|-------------------|
| Meta Alfabetização Brasil | Nacional | taxa, metas 2024-2030, participação |
| Meta Alfabetização UF | Estado | sigla_uf, taxa, metas 2024-2030 |
| Meta Alfabetização Município | Município | id_municipio, taxa, metas, nível |
| Avaliação UF | Estado | taxa, média LP, proporção por nível 0-8 |
| Avaliação Município | Município | taxa, média LP, proporção por nível 0-8 |

### Streaming (ingestão em tempo real)
Microdados da **AEEB 2025** (Avaliação Estadual de Educação Básica) — dados individuais por aluno:

| Campo | Descrição |
|-------|-----------|
| NU_ANO_AVALIACAO | Ano da avaliação |
| CO_UF / SG_UF | Código e sigla do estado |
| ID_ALUNO | Identificador do aluno |
| ID_ESCOLA | Código da escola |
| TP_DEPENDENCIA | Rede (1=Federal, 2=Estadual, 3=Municipal, 4=Privada) |
| CO_MUNICIPIO / NO_MUNICIPIO | Município |
| VL_PROFICIENCIA_LP | Proficiência em Língua Portuguesa |
| IN_ALFABETIZADO | 0=Não, 1=Sim (corte >= 743 na escala SAEB) |

## Fluxo de Dados

### Bronze (Dados Brutos)
- Parquets particionados por `ano` (batch) ou `data_ingestao` (streaming)
- Metadados de ingestão adicionados (`_timestamp_ingestao`, `_fonte`)
- Histórico completo preservado sem alteração

### Silver (Dados Tratados)
- Padronização de tipos (cast para int/double)
- Normalização de campos texto (sigla_uf upper, rede padronizada)
- Deduplicação por chave natural
- Validação de ranges (taxa 0-100, proficiência 400-1000)
- Remoção de registros sem chaves obrigatórias

### Gold (Dados Analíticos)
8 tabelas prontas para consumo:

1. **panorama_alfabetizacao** — Taxa vs meta em todas as granularidades
2. **evolucao_proficiencia** — Distribuição por nível de proficiência
3. **ranking_municipios** — Ranking dentro de cada UF
4. **indicadores_alunos** — Agregação do streaming por UF
5. **indicadores_alunos_municipio** — Agregação por município
6. **desigualdade_regional** — Comparação Norte/Nordeste vs Sul/Sudeste
7. **municipios_criticos** — Municípios com maior gap, classificados por criticidade
8. **tendencia_temporal** — Projeção de atingimento da meta 2030

## Decisões Arquiteturais

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Batch vs Streaming | Ambos (híbrido) | Dados históricos em batch, microdados em streaming |
| Data Lake vs Data Warehouse | Data Lake (S3) | Flexibilidade de schema, custo menor, suporta ML |
| Formato de armazenamento | Parquet + Snappy | Compressão 5-10x, leitura colunar eficiente |
| ETL | AWS Glue (PySpark) | Serverless, sem cluster para gerenciar |
| Streaming | Kinesis + Lambda | Baixa latência, custo proporcional ao uso |
| Consulta | Athena | SQL sobre Parquet, sem servidor, $5/TB escaneado |

Ver detalhes em [Docs/decisoes-arquiteturais.md](Docs/decisoes-arquiteturais.md).

## Monitoramento e FinOps

### Monitoramento
- CloudWatch Logs em todos os Glue Jobs e Lambdas
- Métricas customizadas: registros processados, taxa de erro, latência
- Alertas SNS quando taxa de erro > 5%
- Dead-letter queue (SQS) para eventos com falha

### Otimização de Custos
- Parquet com compressão Snappy em todas as camadas
- Lifecycle policies: Bronze > 90 dias → IA, > 365 dias → Glacier
- Glue workers G.1X (mais baratos), mínimo 2
- Kinesis: 1 shard (escala sob demanda)
- Budget alerts em $50, $80 e $100/mês

**Estimativa mensal: ~$20/mês** (ver [Docs/finops.md](Docs/finops.md))

## Aplicação em Inteligência Artificial

A camada Gold fornece datasets prontos para modelos de ML:

- **Predição de alfabetização** — Prever quais municípios vão/não vão atingir a meta 2030
- **Clustering de vulnerabilidade** — Agrupar municípios por perfil de risco educacional
- **Sistema de alerta precoce** — Identificar escolas/municípios em queda antes do fim do ano
- **Análise de desigualdade** — Quantificar o impacto de variáveis socioeconômicas

A tabela `features_ml` contém variáveis pré-processadas: taxa atual, metas progressivas, gap, percentual de atingimento, nível de alfabetização e participação.

## Estrutura do Repositório

```
Tech-Challenge-Fase2/
├── README.md                          # Este arquivo
├── Docs/
│   ├── decisoes-arquiteturais.md      # Trade-offs técnicos
│   └── finops.md                      # Estimativa e otimização de custos
├── scripts/
│   ├── glue_ingestao_bronze.py        # Ingestão batch → Bronze
│   ├── glue_transformacao_silver.py   # Transformação Bronze → Silver
│   ├── glue_transformacao_gold.py     # Transformação Silver → Gold
│   ├── lambda_consumidora_aeeb.py     # Lambda consumidora (streaming)
│   ├── produtor_streaming_aeeb.py     # Produtor de eventos (streaming)
│   └── validacao_qualidade.py         # Scripts de validação de dados
├── notebooks/
│   └── analise_gold.ipynb             # Visualizações e insights
├── tabelas_gold/
│   ├── README_GOLD.md                 # Documentação das tabelas Gold
│   └── *.csv                          # Dados exportados da Gold
├── dados/                             # Dados fonte (CSVs da Base dos Dados)
└── proposta/                          # Enunciado do Tech Challenge
```

## Como Executar

### Pré-requisitos
- Conta AWS com acesso a S3, Glue, Kinesis, Lambda, Athena
- AWS CLI configurado (`aws configure`)
- Python 3.10+ (para produtor local e notebooks)

### Passo a passo
1. Criar bucket S3: `tech-challange-fase2`
2. Upload dos CSVs para `s3://bucket/raw-csv/`
3. Executar Glue Job: `glue_ingestao_bronze.py` (popula Bronze)
4. Criar Kinesis stream: `alfabetizacao-events`
5. Deploy Lambda consumidora: `lambda_consumidora_aeeb.py`
6. Executar produtor: `python scripts/produtor_streaming_aeeb.py`
7. Executar Glue Job: `glue_transformacao_silver.py` (Bronze → Silver)
8. Executar Glue Job: `glue_transformacao_gold.py` (Silver → Gold)
9. Criar Crawlers para catalogar Silver e Gold
10. Consultar via Athena

## Tecnologias Utilizadas

- **AWS S3** — Armazenamento do Data Lake
- **AWS Glue** — ETL serverless (PySpark)
- **AWS Kinesis** — Streaming de dados
- **AWS Lambda** — Processamento de eventos
- **AWS Athena** — Consultas SQL
- **AWS EventBridge** — Agendamento
- **AWS CloudWatch** — Monitoramento
- **Python / PySpark** — Linguagem principal
- **Parquet** — Formato de armazenamento
- **Pandas / Matplotlib / Seaborn** — Análise e visualização

## Equipe
- Gabriel Valentim De Oliveira Dacie
- Henrique Ribeiro Rodrigues
- Nataly Mafra Girotti
- Samuel Nunes de Oliveira Correia

Tech Challenge — Fase 2 | Pós-graduação em Ciência de dados

