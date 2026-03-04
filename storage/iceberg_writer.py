import os
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from datetime import datetime

# Diretório local para simular object storage (GCS/S3 em produção)
WAREHOUSE_PATH = os.path.abspath("./storage/warehouse")
os.makedirs(WAREHOUSE_PATH, exist_ok=True)

def get_catalog():
    """Retorna o catálogo Iceberg — equivalente ao Glue/BigQuery Metastore"""
    catalog = SqlCatalog(
        "wikipedia_catalog",
        **{
            "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
            "warehouse": f"file://{WAREHOUSE_PATH}",
        }
    )
    return catalog

def criar_tabela_se_nao_existe(catalog):
    """Cria a tabela Iceberg com schema definido"""
    
    schema = pa.schema([
        pa.field("window_start", pa.string()),
        pa.field("window_end", pa.string()),
        pa.field("wiki", pa.string()),
        pa.field("total_edicoes", pa.int64()),
        pa.field("data_particao", pa.string()),
    ])
    
    if not catalog.table_exists("wikipedia.stats"):
        catalog.create_namespace("wikipedia")
        catalog.create_table(
            "wikipedia.stats",
            schema=schema,
            properties={
                "write.parquet.compression-codec": "snappy"
            }
        )
        print("✅ Tabela wikipedia.stats criada")
    
    return catalog.load_table("wikipedia.stats")

def salvar_resultado_janela(contagem, window_start, window_end):
    """Salva resultado de uma janela Flink no Iceberg"""
    
    catalog = get_catalog()
    tabela = criar_tabela_se_nao_existe(catalog)
    
    inicio_str = datetime.fromtimestamp(window_start).strftime('%Y-%m-%d %H:%M:%S')
    fim_str = datetime.fromtimestamp(window_end).strftime('%Y-%m-%d %H:%M:%S')
    data_str = datetime.fromtimestamp(window_start).strftime('%Y-%m-%d')
    
    # Converte contagem para DataFrame
    registros = [
        {
            "window_start": inicio_str,
            "window_end": fim_str,
            "wiki": wiki,
            "total_edicoes": count,
            "data_particao": data_str
        }
        for wiki, count in contagem.items()
    ]
    
    df = pd.DataFrame(registros)
    arrow_table = pa.Table.from_pandas(df)
    
    # Escreve no Iceberg — ACID transaction
    tabela.append(arrow_table)
    
    print(f"💾 {len(registros)} registros salvos no Iceberg")
    return len(registros)