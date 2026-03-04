import json
import time
import requests
from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(config)

def delivery_report(err, msg):
    if err:
        print(f'Erro na entrega: {err}')
    else:
        print(f'Mensagem entregue → topic: {msg.topic()} | partition: {msg.partition()} | offset: {msg.offset()}')

def buscar_eventos_wikipedia():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    headers = {
        'User-Agent': 'streaming-pipeline-study/1.0 (leo.br.almeida@gmail.com)'
    }
    eventos = []
    
    with requests.get(url, stream=True, timeout=15, headers=headers) as response:
        for i, line in enumerate(response.iter_lines()):
            if i >= 100:
                break
            if line:
                line = line.decode('utf-8')
                if line.startswith('data:'):
                    try:
                        evento = json.loads(line[5:])
                        eventos.append(evento)
                    except:
                        pass
    return eventos

def publicar_eventos():
    print("Producer iniciado — publicando eventos da Wikipedia...")
    
    while True:
        try:
            eventos = buscar_eventos_wikipedia()
            
            for evento in eventos:
                payload = {
                    'titulo': evento.get('title'),
                    'wiki': evento.get('wiki'),
                    'usuario': evento.get('user'),
                    'timestamp': evento.get('timestamp'),
                    'tipo': evento.get('type'),
                    'tamanho_novo': evento.get('length', {}).get('new'),
                    'tamanho_antigo': evento.get('length', {}).get('old'),
                }
                
                producer.produce(
                    topic='wikipedia-events',
                    key=str(payload['wiki']),
                    value=json.dumps(payload),
                    callback=delivery_report
                )
            
            producer.flush()
            print(f"{len(eventos)} eventos publicados")
            time.sleep(30)
            
        except Exception as e:
            print(f"Erro: {e}")
            time.sleep(5)

if __name__ == "__main__":
    publicar_eventos()