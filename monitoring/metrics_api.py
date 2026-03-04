import os
import sys
import duckdb
from flask import Flask, jsonify

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)
@app.route('/')
def health():
    return jsonify({"status": "ok", "service": "wikipedia-metrics-api"})
WAREHOUSE_PATH = os.path.abspath("./storage/warehouse")

def query_iceberg(sql):
    con = duckdb.connect()
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")
    con.execute("SET unsafe_enable_version_guessing = true")
    return con.execute(sql).fetchdf()

@app.route('/metrics/ranking')
def ranking():
    try:
        df = query_iceberg(f"""
            SELECT 
                wiki,
                SUM(total_edicoes) as total_geral,
                COUNT(*) as janelas_ativa
            FROM iceberg_scan('{WAREHOUSE_PATH}/wikipedia/stats')
            GROUP BY wiki
            ORDER BY total_geral DESC
            LIMIT 10
        """)
        # Formato que o Grafana JSON API espera
        return jsonify({
            "data": df.to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/metrics/timeline')
def timeline():
    try:
        df = query_iceberg(f"""
            SELECT 
                window_start,
                wiki,
                total_edicoes
            FROM iceberg_scan('{WAREHOUSE_PATH}/wikipedia/stats')
            ORDER BY window_start DESC
            LIMIT 50
        """)
        return jsonify(df.to_dict(orient='records'))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/metrics/summary')
def summary():
    try:
        df = query_iceberg(f"""
            SELECT 
                COUNT(DISTINCT wiki) as total_wikis,
                SUM(total_edicoes) as total_edicoes,
                COUNT(DISTINCT window_start) as total_janelas
            FROM iceberg_scan('{WAREHOUSE_PATH}/wikipedia/stats')
        """)
        row = df.iloc[0]
        return jsonify({
            "total_wikis": int(row['total_wikis']),
            "total_edicoes": int(row['total_edicoes']),
            "total_janelas": int(row['total_janelas'])
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print("🚀 Metrics API rodando em http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False)