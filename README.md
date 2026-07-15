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
              │  9 tabelas analíticas prontas para consumo      │
              └────────────────────────┬───────────────────────┘
                                       ▼
              ┌────────────┐  ┌────────────┐  ┌────────────┐
              │   Athena   │  │ QuickSight │  │ SageMaker  │
              │  (Queries) │  │(Dashboards)│  │   (ML)     │
              └────────────┘  └────────────┘  └────────────┘
```

### Orquestração

O pipeline é orquestrado por **AWS Step Functions** com dois fluxos distintos:

- **Batch (Step Function):** Executa sequencialmente `Raw → Bronze → Silver → Gold`. Triggered por EventBridge ao detectar novos objetos no prefixo `Raw/` do S3.
- **Streaming (Step Function):** Executa `Athena REPAIR → Silver → Athena REPAIR → Gold`. Triggered por EventBridge ao detectar novos objetos no prefixo `Bronze/` do S3 (escritos pela Lambda consumidora).

Ambos possuem tratamento de erro com estado `Falha_Pipeline` (tipo Fail) que captura erros de qualquer etapa.

### Componentes AWS Utilizados

| Serviço | Função | Justificativa |
|---------|--------|---------------|
| S3 | Data Lake (Bronze/Silver/Gold) | Custo baixo, escalável, suporta Parquet |
| AWS Glue | ETL (Spark) — transformações batch | Serverless, paga por uso, suporta PySpark |
| Kinesis Data Streams | Buffer de streaming | Baixa latência, integração nativa com Lambda |
| Lambda | Consumidora de streaming | Serverless, escala automática, custo mínimo |
| Step Functions | Orquestração dos pipelines | Coordena jobs sequenciais, tratamento de erro nativo |
| EventBridge | Detecção de eventos S3 + agendamento | Trigger automático sem polling, sem infraestrutura |
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

O produtor de streaming (`produtor_streaming_aeeb.py`) simula a geração desses microdados e publica no Kinesis. A Lambda consumidora grava em Parquet na Bronze particionado por `data_ingestao`. Adicionalmente, o CSV completo da AEEB (`TS_ALUNO.csv`) também é ingerido via batch na Bronze com partição por `data_ingestao`.

## Fluxo de Dados

### Bronze (Dados Brutos)
- Parquets particionados por `ano` (batch) ou `data_ingestao` (streaming/TS_ALUNO)
- Metadados de ingestão adicionados (`_timestamp_ingestao`, `_fonte`, `_arquivo_original`)
- Histórico completo preservado sem alteração

### Silver (Dados Tratados)
- Padronização de tipos (cast para int/double)
- Normalização de campos texto (sigla_uf upper, rede padronizada)
- Deduplicação por chave natural
- Validação de ranges (taxa 0-100, proficiência 400-1000)
- Remoção de registros sem chaves obrigatórias
- Padronização de códigos de rede (2=Estadual, 3=Municipal, 5=Publica, 0=Total)

### Gold (Dados Analíticos)
9 tabelas prontas para consumo:

1. **panorama_alfabetizacao** — Taxa vs meta em todas as granularidades (Brasil + UF + Município), com gap e percentual de atingimento
2. **evolucao_proficiencia** — Distribuição por nível de proficiência (nível 0 a 8) por UF e ano
3. **ranking_municipios** — Ranking de municípios por taxa dentro de cada UF, com percentil
4. **indicadores_alunos** — Agregação do streaming por UF/rede (total avaliados, alfabetizados, proficiência)
5. **indicadores_alunos_municipio** — Agregação do streaming por município/rede
6. **features_ml** — Dataset pronto para ML com taxa, metas progressivas, gap, atingimento, nível
7. **desigualdade_regional** — Comparação entre regiões (Norte/Nordeste vs Sul/Sudeste/Centro-Oeste)
8. **municipios_criticos** — Municípios com maior gap, classificados por nível de criticidade
9. **tendencia_temporal** — Variação ano-a-ano por UF e projeção de atingimento da meta 2030

## Decisões Arquiteturais

| Decisão | Escolha | Justificativa |
|---------|---------|---------------|
| Batch vs Streaming | Ambos (híbrido) | Dados históricos em batch, microdados em streaming |
| Data Lake vs Data Warehouse | Data Lake (S3) | Flexibilidade de schema, custo menor, suporta ML |
| Formato de armazenamento | Parquet + Snappy | Compressão 5-10x, leitura colunar eficiente |
| ETL | AWS Glue (PySpark) | Serverless, sem cluster para gerenciar |
| Streaming | Kinesis + Lambda | Baixa latência, custo proporcional ao uso |
| Orquestração | Step Functions | Coordenação sequencial, error handling, integração nativa |
| Detecção de eventos | EventBridge | Trigger por evento S3, sem polling |
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

**Estimativa mensal: ~$22/mês** (ver [Docs/finops.md](Docs/finops.md))

## Aplicação em Inteligência Artificial

A camada Gold fornece datasets prontos para modelos de ML:

- **Predição de alfabetização** — Prever quais municípios vão/não vão atingir a meta 2030
- **Clustering de vulnerabilidade** — Agrupar municípios por perfil de risco educacional
- **Sistema de alerta precoce** — Identificar escolas/municípios em queda antes do fim do ano
- **Análise de desigualdade** — Quantificar o impacto de variáveis socioeconômicas

A tabela `features_ml` contém variáveis pré-processadas: taxa atual, metas progressivas (2024-2030), gap, percentual de atingimento, nível de alfabetização, participação e flag de meta já atingida.

## Estrutura do Repositório

```
Tech-Challenge-Fase2/
├── README.md                              # Este arquivo
├── Docs/
│   ├── decisoes-arquiteturais.md          # Trade-offs técnicos (ADRs)
│   └── finops.md                          # Estimativa e otimização de custos
├── Scripts_glue/
│   ├── glue_ingestao_bronze.py            # Ingestão Raw → Bronze (6 fontes)
│   ├── glue_transformacao_silver.py       # Transformação Bronze → Silver
│   ├── glue_transformacao_gold.py         # Transformação Silver → Gold (9 tabelas)
│   ├── lambda_consumidora_aeeb.py         # Lambda consumidora (streaming → Bronze)
│   ├── produtor_streaming_aeeb.py         # Produtor de eventos Kinesis (simulador)
│   └── validacao_qualidade.py             # Validação de qualidade na Silver
├── Step_function/
│   ├── batch                              # Definição Step Function batch (Raw→Bronze→Silver→Gold)
│   └── streaming                          # Definição Step Function streaming (Repair→Silver→Repair→Gold)
├── event_bridge/
│   ├── detector_raw                       # Regra EventBridge: detecta objetos em Raw/
│   └── detector_streaming                 # Regra EventBridge: detecta objetos em Bronze/
├── Analises/
    └── analise_gold.ipynb                 # Visualizações e insights da camada Gold
```

## Como Executar

### Pré-requisitos
- Conta AWS com acesso a S3, Glue, Kinesis, Lambda, Athena, Step Functions, EventBridge
- AWS CLI configurado (`aws configure`)
- Python 3.10+ (para produtor local e notebooks)
- Bibliotecas Python: `boto3`, `pandas`, `matplotlib`, `seaborn` (para notebooks)

### Passo a passo

#### Pipeline Batch
1. Criar bucket S3: `tech-challange-fase2`
2. Upload dos CSVs para `s3://tech-challange-fase2/Raw/<nome_fonte>/`
3. Criar Glue Jobs com os scripts da pasta `Scripts_glue/`
4. Criar Step Function batch (usar definição em `Step_function/batch`)
5. Criar regra EventBridge com pattern de `event_bridge/detector_raw` → target: Step Function batch
6. Upload de CSVs no prefixo `Raw/` dispara automaticamente o pipeline completo

#### Pipeline Streaming
7. Criar Kinesis Data Stream: `alfabetizacao-events` (1 shard)
8. Deploy Lambda consumidora: `Scripts_glue/lambda_consumidora_aeeb.py` (trigger: Kinesis)
9. Criar Step Function streaming (usar definição em `Step_function/streaming`)
10. Criar regra EventBridge com pattern de `event_bridge/detector_streaming` → target: Step Function streaming
11. Executar produtor: `python Scripts_glue/produtor_streaming_aeeb.py --quantidade 1000 --lote`

#### Consulta e Análise
12. Criar Crawlers para catalogar Bronze, Silver e Gold no Glue Catalog
13. Consultar via Athena (tabelas registradas automaticamente pelo Crawler)
14. Usar notebook `Analises/analise_gold.ipynb` para visualizações

## Validação de Qualidade

O script `validacao_qualidade.py` executa 6 categorias de validação na camada Silver:

1. Campos obrigatórios não nulos
2. Ranges de valores (taxa 0-100, proficiência 400-1000)
3. Unicidade por chave natural (sem duplicatas)
4. Metas monotonicamente crescentes (2024 ≤ 2025 ≤ ... ≤ 2030)
5. Proporções somam ~100% (tolerância ±2%)
6. Consistência entre tabelas (UFs presentes em meta e avaliação)

## Tecnologias Utilizadas

- **AWS S3** — Armazenamento do Data Lake
- **AWS Glue** — ETL serverless (PySpark)
- **AWS Kinesis** — Streaming de dados
- **AWS Lambda** — Processamento de eventos
- **AWS Step Functions** — Orquestração de pipelines
- **AWS EventBridge** — Detecção de eventos e agendamento
- **AWS Athena** — Consultas SQL
- **AWS CloudWatch** — Monitoramento
- **Python / PySpark** — Linguagem principal
- **PyArrow** — Serialização Parquet na Lambda
- **Parquet** — Formato de armazenamento
- **Pandas / Matplotlib / Seaborn** — Análise e visualização

## Equipe
- Gabriel Valentim De Oliveira Dacie
- Henrique Ribeiro Rodrigues
- Nataly Mafra Girotti
- Samuel Nunes de Oliveira Correia

Tech Challenge — Fase 2 | Pós-graduação em Ciência de Dados
