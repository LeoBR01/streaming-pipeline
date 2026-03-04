import json
import time
from datetime import datetime
from collections import defaultdict
from confluent_kafka import Consumer, KafkaException

# Configuração do consumer Flink-style
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'flink-wikipedia-processor',
    'auto.offset.reset': 'latest'
}

WINDOW_SIZE = 60  # segundos

def processar_stream():
    consumer = Consumer(config)
    consumer.subscribe(['wikipedia-events'])
    
    print("🚀 Flink Job iniciado — processando stream da Wikipedia...")
    print(f"📊 Tumbling Window: {WINDOW_SIZE} segundos\n")
    
    # Estado da janela atual
    window_start = time.time()
    contagem = defaultdict(int)  # wiki → número de edições
    
    try:
        while True:
            msg = consumer.poll(timeout=0.1)
            
            if msg and not msg.error():
                evento = json.loads(msg.value().decode('utf-8'))
                wiki = evento.get('wiki', 'unknown')
                contagem[wiki] += 1
            
            # Verifica se a janela fechou
            agora = time.time()
            if agora - window_start >= WINDOW_SIZE:
                emitir_resultado(contagem, window_start, agora)
                # Reset da janela
                contagem = defaultdict(int)
                window_start = agora
                
    except KeyboardInterrupt:
        print("\n⛔ Job encerrado")
    finally:
        consumer.close()

def emitir_resultado(contagem, window_start, window_end):
    inicio = datetime.fromtimestamp(window_start).strftime('%H:%M:%S')
    fim = datetime.fromtimestamp(window_end).strftime('%H:%M:%S')
    
    print(f"\n{'='*50}")
    print(f"📊 Janela: {inicio} → {fim}")
    print(f"{'='*50}")
    
    if not contagem:
        print("Nenhum evento nessa janela")
        return
    
    # Ordena por número de edições
    ranking = sorted(contagem.items(), key=lambda x: x[1], reverse=True)
    total = sum(contagem.values())
    
    for i, (wiki, count) in enumerate(ranking[:10], 1):
        barra = '█' * min(count, 40)
        print(f"{i:2}. {wiki:<20} {barra} {count}")
    
    print(f"\n📈 Total de edições: {total}")
    print(f"🌍 Wikis ativas: {len(contagem)}")

if __name__ == "__main__":
    processar_stream()