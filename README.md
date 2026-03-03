# 🚀 Real-Time Streaming Pipeline

Pipeline de dados em tempo real para ingestão, processamento e armazenamento de eventos de transporte urbano do Rio de Janeiro.

## Arquitetura
```
Fonte (API RJ) → Kafka → Flink → Apache Iceberg → DuckDB → Grafana
```

## Stack
- **Kafka** — ingestão e transporte de eventos
- **Apache Flink** — processamento em tempo real
- **Apache Iceberg** — armazenamento em lakehouse moderno
- **DuckDB** — consultas analíticas
- **Grafana + Prometheus** — observabilidade
- **Docker Compose** — orquestração local

## Como rodar
```bash
docker compose up -d
```

## Estrutura
```
producer/    # Kafka producers
consumer/    # Kafka consumers  
processing/  # Flink jobs
storage/     # Iceberg config
monitoring/  # Grafana + Prometheus
docs/        # Arquitetura e decisões de design
```