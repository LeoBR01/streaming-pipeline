import duckdb
import os

WAREHOUSE_PATH = os.path.abspath("./storage/warehouse")

con = duckdb.connect()

con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")
con.execute("SET unsafe_enable_version_guessing = true")

result = con.execute(f"""
    SELECT 
        window_start,
        window_end,
        wiki,
        total_edicoes
    FROM iceberg_scan('{WAREHOUSE_PATH}/wikipedia/stats')
    ORDER BY total_edicoes DESC
""").fetchdf()

print("📊 Dados salvos no Iceberg:")
print(result.to_string(index=False))

print(f"\n📈 Total de janelas processadas: {result['window_start'].nunique()}")
print(f"🌍 Total de wikis vistas: {result['wiki'].nunique()}")
print("\n📊 Ranking geral por wiki (todas as janelas):")
ranking = con.execute(f"""
    SELECT 
        wiki,
        SUM(total_edicoes) as total_geral,
        COUNT(*) as janelas_ativa
    FROM iceberg_scan('{WAREHOUSE_PATH}/wikipedia/stats')
    GROUP BY wiki
    ORDER BY total_geral DESC
""").fetchdf()
print(ranking.to_string(index=False))