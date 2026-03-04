# 🚀 Real-Time Streaming Pipeline

Pipeline de dados em tempo real para ingestão, processamento e armazenamento de eventos — usando edições da Wikipedia como fonte de dados.

## Arquitetura
```
Wikipedia Stream → Kafka → Flink → Apache Iceberg → DuckDB → Grafana
```

## Stack
- **Kafka** — ingestão e transporte de eventos
- **Apache Flink** — processamento em tempo real
- **Apache Iceberg** — armazenamento em lakehouse moderno
- **DuckDB** — consultas analíticas
- **Grafana + Prometheus** — observabilidade
- **Docker Compose** — orquestração local

## Status do Projeto
- [x] Ambiente Docker com Kafka + Zookeeper + Flink
- [x] Producer Python consumindo Wikipedia EventStream em tempo real
- [x] Consumer Python lendo eventos do Kafka
- [x] Flink job com Tumbling Window agregando edições por wiki
- [x] Apache Iceberg — persistência ACID com time travel
- [x] DuckDB — consultas analíticas no lakehouse
- [ ] Observabilidade com Grafana + Prometheus

## Como rodar

**Subir infraestrutura:**
```bash
docker compose up -d
```

**Rodar o producer:**
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python producer/producer.py
```

## Estrutura
```
producer/    # Kafka producer — Wikipedia EventStream
consumer/    # Kafka consumer  
processing/  # Flink jobs
storage/     # Iceberg config
monitoring/  # Grafana + Prometheus
docs/        # Arquitetura e decisões de design
```