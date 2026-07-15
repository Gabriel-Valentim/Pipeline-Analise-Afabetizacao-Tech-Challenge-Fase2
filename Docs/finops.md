# FinOps — Otimização de Custos

## Estimativa de Custo Mensal

| Serviço | Uso Estimado | Custo/Mês |
|---------|-------------|-----------|
| S3 (armazenamento) | ~1 GB total | $0.02 |
| S3 (requests PUT) | ~5000 requests/mês | $0.03 |
| AWS Glue | 3 jobs × 2 DPU × 5 min | $4.40 |
| Kinesis Data Streams | 1 shard, 24h retenção | $15.00 |
| Lambda | ~5000 invocações × 256MB × 1s | $0.00 (free tier) |
| Step Functions | ~100 transições/mês | $0.00 (free tier: 4000/mês) |
| Athena | ~10 GB scan/mês | $0.05 |
| CloudWatch | Logs + 5 métricas | $1.00 |
| SQS (DLQ) | ~100 mensagens | $0.00 |
| EventBridge | 2 regras S3, ~500 eventos/mês | $0.00 |
| **TOTAL** | | **~$20.50/mês** |

## Práticas de Otimização Implementadas

### 1. Formato de Armazenamento
- **Parquet + Snappy** em todas as camadas
- Redução de ~80% no tamanho comparado a CSV
- Leitura colunar reduz scan do Athena (economia de $5/TB)

### 2. Particionamento
- Batch: particionado por `ano` → Athena escaneia só a partição relevante
- Streaming: particionado por `data_ingestao` → facilita lifecycle e reprocessamento

### 3. Lifecycle Policies (S3)
| Regra | Transição | Economia |
|-------|-----------|----------|
| Bronze > 90 dias | S3 Standard → S3 Infrequent Access | ~40% |
| Bronze > 365 dias | S3 IA → S3 Glacier | ~80% |
| Athena results > 7 dias | Deletar | 100% |

### 4. Compute
- Glue: workers **G.1X** (mais baratos que G.2X), mínimo 2
- Lambda: **256 MB** (suficiente para processar JSON + Parquet via PyArrow)
- Kinesis: **1 shard** (escala para 1000 records/s, muito além do necessário)
- Step Functions: Standard Workflow (free tier cobre 4000 transições/mês)

### 5. Orquestração Event-Driven
- EventBridge detecta objetos novos no S3 → pipeline só executa quando há dados
- Elimina execuções desnecessárias (vs cron schedule fixo)
- Step Functions com `.sync` evita polling ativo durante execução dos Glue Jobs

### 6. Monitoramento de Custos
- AWS Budgets configurado com alertas em:
  - 50% do orçamento ($10)
  - 80% do orçamento ($16)
  - 100% do orçamento ($20)
- SNS notifica o time quando limites são atingidos

### 7. Queries Athena
- Sempre especificar colunas (evitar `SELECT *`)
- Usar filtro de partição (`WHERE ano = 2024`)
- Parquet colunar = só escaneia colunas solicitadas
- MSCK REPAIR TABLE automatizado via Step Function (detecta novas partições)

## Como a Arquitetura foi Otimizada

1. **Serverless first** — Sem servidores ligados 24/7. Glue, Lambda, Step Functions e Athena cobram apenas pelo uso
2. **Event-driven** — EventBridge dispara pipelines só quando há dados novos (zero desperdício)
3. **Right-sizing** — Kinesis com 1 shard, Glue com 2 workers, Lambda com 256 MB
4. **Formato eficiente** — Parquet reduz armazenamento e custo de query
5. **Lifecycle automático** — Dados antigos migram automaticamente para storage mais barato
6. **Pay-per-query** — Athena cobra $5/TB. Com Parquet particionado, cada query custa frações de centavo
7. **Free tier aproveitado** — Step Functions (4000 transições), Lambda (1M invocações), EventBridge (regras S3 gratuitas)

## Decisões que Reduzem Custos

| Decisão | Alternativa cara | Economia |
|---------|-----------------|----------|
| S3 + Athena | Redshift ($180/mês) | ~$160/mês |
| Glue (serverless) | EMR cluster ($100/mês) | ~$95/mês |
| Kinesis 1 shard | MSK Kafka ($200/mês) | ~$185/mês |
| Lambda | EC2 t3.medium ($30/mês) | ~$30/mês |
| Step Functions | Airflow/MWAA ($350/mês) | ~$350/mês |
| EventBridge | Polling com Lambda ($5/mês) | ~$5/mês |
| **Total economizado** | | **~$825/mês** |

## Custo por Cenário de Escala

| Cenário | Volume | Custo Estimado |
|---------|--------|----------------|
| Dev/Teste (atual) | 1 GB, 50k registros | ~$20/mês |
| Produção estadual | 10 GB, 500k registros | ~$45/mês |
| Produção nacional | 100 GB, 5M registros | ~$120/mês |

A arquitetura escala linearmente com o volume sem necessidade de re-arquitetar.
