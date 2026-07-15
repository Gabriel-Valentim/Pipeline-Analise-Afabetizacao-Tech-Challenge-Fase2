# Decisões Arquiteturais

## ADR-001: Arquitetura Híbrida (Batch + Streaming)

**Contexto:** O pipeline precisa processar dados históricos (CSVs da Base dos Dados) e simular ingestão em tempo real (microdados de alunos).

**Decisão:** Implementar ingestão híbrida — Glue Jobs para batch e Kinesis + Lambda para streaming.

**Trade-offs:**
- Batch: mais simples, processamento completo, ideal para dados que mudam raramente
- Streaming: complexidade maior, mas permite near real-time e demonstra capacidade de escala
- Híbrido: combina o melhor dos dois, mas exige dois fluxos de manutenção

**Alternativa descartada:** Apenas batch com schedule frequente (a cada hora). Não demonstraria capacidade de streaming.

---

## ADR-002: Data Lake (S3) vs Data Warehouse

**Contexto:** Escolher onde armazenar os dados processados.

**Decisão:** Data Lake em S3 com catálogo Glue + consultas via Athena.

**Trade-offs:**
- Data Lake: flexível, schema-on-read, suporta ML diretamente, custo por armazenamento
- Data Warehouse (Redshift): schema-on-write, melhor para BI complexo, custo fixo alto
- Escolha S3: nosso volume é pequeno (~1 GB), Athena cobre as consultas, e ML lê direto do S3

**Alternativa descartada:** Amazon Redshift — custo mínimo de ~$180/mês vs $0.02/mês no S3.

---

## ADR-003: Formato Parquet com Compressão Snappy

**Contexto:** Definir formato de armazenamento nas camadas Bronze/Silver/Gold.

**Decisão:** Parquet com compressão Snappy em todas as camadas.

**Justificativa:**
- Parquet é colunar → Athena escaneia só as colunas necessárias (economia de custo)
- Snappy oferece bom equilíbrio compressão/velocidade (5-10x menor que CSV)
- Suportado nativamente por Glue, Athena, Spark, Pandas
- Preserva tipos nativos (int, double, string) sem ambiguidade

**Alternativa descartada:** CSV (lento, sem tipos) ou ORC (menos compatibilidade com ecossistema Python).

---

## ADR-004: AWS Glue vs EMR vs Lambda para ETL

**Contexto:** Escolher engine de processamento para transformações.

**Decisão:** AWS Glue (Spark serverless).

**Trade-offs:**
- Glue: serverless (sem cluster), paga por DPU-hora, escala automática, ideal para jobs esporádicos
- EMR: mais controle, mais barato para cargas contínuas, mas exige gerenciamento de cluster
- Lambda: limitada a 15min e 10GB RAM, não suporta Spark

**Justificativa:** Volume de dados pequeno (~5000 registros batch + ~50k streaming). Glue processa em <5 minutos com 2 workers. Não justifica um cluster EMR.

---

## ADR-005: Kinesis vs Kafka (MSK) vs SQS para Streaming

**Contexto:** Escolher serviço de streaming para ingestão de microdados.

**Decisão:** Kinesis Data Streams.

**Trade-offs:**
- Kinesis: managed, integração nativa com Lambda, 1 shard = $15/mês, simples
- MSK (Kafka): mais poderoso, mais caro (~$200/mês mínimo), overkill para nosso volume
- SQS: não garante ordem, não é streaming de verdade, mais indicado para filas de trabalho

**Justificativa:** 1 shard de Kinesis suporta 1000 records/segundo — muito além do necessário. Integração direta com Lambda sem configuração adicional.

---

## ADR-006: Particionamento por Ano vs Data

**Contexto:** Definir estratégia de particionamento no S3.

**Decisão:**
- Batch (Bronze/Silver/Gold): particionado por `ano`
- Streaming (Bronze): particionado por `data_ingestao` (dia)

**Justificativa:**
- Queries mais comuns filtram por ano → partição por ano reduz scan em ~70%
- Streaming gera dados diários → partição por dia facilita reprocessamento e lifecycle

---

## ADR-007: Validação no Pipeline vs Pós-Processamento

**Contexto:** Onde aplicar regras de qualidade de dados.

**Decisão:** Validação inline na transformação Bronze → Silver (fail-fast).

**Justificativa:**
- Registros inválidos são filtrados antes de chegar na Silver
- Gold só recebe dados já validados → confiabilidade para dashboards e ML
- Logs de validação permitem auditoria do que foi descartado

**Regras implementadas:**
- Taxa de alfabetização: range [0, 100]
- Proficiência: range [400, 1000]
- Dependência administrativa: valores [1, 2, 3, 4]
- Chaves primárias: não nulas
- Deduplicação por chave natural

---

## ADR-008: Step Functions para Orquestração

**Contexto:** Coordenar a execução sequencial dos Glue Jobs (Bronze → Silver → Gold) garantindo que cada etapa só execute após o sucesso da anterior, com tratamento de falhas centralizado.

**Decisão:** AWS Step Functions com dois state machines distintos (batch e streaming).

**Trade-offs:**
- Step Functions: visual, error handling nativo, integração direta com Glue (`.sync`), logs automáticos
- Airflow (MWAA): mais flexível para DAGs complexos, mas custo mínimo ~$350/mês e overhead operacional
- Glue Workflows: limitado a Glue Jobs, sem integração com Athena ou Lambda
- EventBridge Pipes: bom para encadeamento simples, mas menos controle sobre retry e branching

**Justificativa:** Pipeline sequencial simples (3-4 etapas), custo praticamente zero no nosso volume (<1000 transições/mês = free tier), integração nativa com Glue `.sync` (espera job finalizar) e Athena `startQueryExecution.sync`.

**Implementação:**
- **Batch:** `Raw → Bronze → Silver → Gold` (3 Glue Jobs em sequência)
- **Streaming:** `Athena REPAIR Bronze → Silver → Athena REPAIR Silver → Gold` (intercala REPAIR para detectar novas partições criadas pela Lambda)

---

## ADR-009: EventBridge para Detecção de Eventos S3

**Contexto:** Definir como disparar os pipelines automaticamente quando novos dados chegam ao S3.

**Decisão:** Amazon EventBridge com regras que detectam `Object Created` em prefixos específicos do S3.

**Trade-offs:**
- EventBridge: nativo AWS, sem custo para regras S3, filtragem por prefixo, target direto para Step Functions
- S3 Event Notifications: funcional mas limitado a Lambda/SQS/SNS como destino direto
- Polling periódico (cron): simples mas desperdiça execuções quando não há dados novos

**Justificativa:** EventBridge é event-driven puro — o pipeline só executa quando há dados novos. Zero custo adicional para detecção de eventos S3. Suporta pattern matching no prefixo do objeto.

**Regras implementadas:**
- `detector_raw`: detecta objetos criados em `s3://tech-challange-fase2/Raw/` → dispara Step Function batch
- `detector_streaming`: detecta objetos criados em `s3://tech-challange-fase2/Bronze/` → dispara Step Function streaming

---

## ADR-010: PyArrow na Lambda vs Pandas/Spark

**Contexto:** A Lambda consumidora precisa converter JSON do Kinesis para Parquet e gravar no S3. Lambda tem limite de 256MB de RAM e 15min de timeout.

**Decisão:** Usar PyArrow nativo para serialização Parquet na Lambda.

**Trade-offs:**
- PyArrow: leve (~30MB), rápido, escrita Parquet direta sem overhead de framework
- Pandas: funcional mas mais pesado em memória, desnecessário para conversão simples
- AWS Glue (Spark): não cabe em Lambda, overkill para micro-batches de dezenas de registros

**Justificativa:** PyArrow permite definir schema tipado explícito, é a dependência mais leve para escrita Parquet, e processa lotes do Kinesis em <1 segundo com 256MB de RAM.
