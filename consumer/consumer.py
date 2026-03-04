import json
from confluent_kafka import Consumer, KafkaException

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'wikipedia-consumer-group',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(config)
consumer.subscribe(['wikipedia-events'])

print("Consumer iniciado — aguardando eventos...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
            
        if msg.error():
            raise KafkaException(msg.error())
        
        evento = json.loads(msg.value().decode('utf-8'))
        
        print(f"Wiki: {evento.get('wiki'):<20} | "
              f"Título: {evento.get('titulo'):<50} | "
              f"Usuário: {evento.get('usuario')}")

except KeyboardInterrupt:
    print("\nConsumer encerrado")
finally:
    consumer.close()