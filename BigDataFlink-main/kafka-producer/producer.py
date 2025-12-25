import csv
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'mock_data_topic')
DATA_DIR = '/data'
DELAY_BETWEEN_MESSAGES = 0.01

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def csv_to_dict(row, headers):
    return dict(zip(headers, row))

def send_csv_to_kafka(producer, csv_file_path):
    print(f"Обработка файла: {csv_file_path}")
    
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        row_count = 0
        for row in reader:
            if len(row) != len(headers):
                continue
            
            data = csv_to_dict(row, headers)
            
            future = producer.send(KAFKA_TOPIC, value=data, key=str(data.get('id', '')))
            
            try:
                record_metadata = future.get(timeout=10)
                row_count += 1
                if row_count % 100 == 0:
                    print(f"Отправлено {row_count} сообщений из {csv_file_path}")
            except KafkaError as e:
                print(f"Ошибка при отправке: {e}")
            
            time.sleep(DELAY_BETWEEN_MESSAGES)
    
    print(f"Завершено: отправлено {row_count} сообщений из {csv_file_path}")
    return row_count

def main():
    print("Запуск Kafka Producer...")
    print(f"Kafka сервер: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Топик: {KAFKA_TOPIC}")
    
    print("Ожидание готовности Kafka...")
    time.sleep(5)
    
    producer = create_producer()
    
    csv_files = sorted([
        os.path.join(DATA_DIR, f) 
        for f in os.listdir(DATA_DIR) 
        if f.endswith('.csv') and f.startswith('MOCK_DATA')
    ])
    
    if not csv_files:
        print(f"CSV файлы не найдены в {DATA_DIR}")
        return
    
    print(f"Найдено {len(csv_files)} CSV файлов")
    
    total_sent = 0
    for csv_file in csv_files:
        try:
            count = send_csv_to_kafka(producer, csv_file)
            total_sent += count
        except Exception as e:
            print(f"Ошибка при обработке {csv_file}: {e}")
    
    producer.flush()
    producer.close()
    
    print(f"\nВсего отправлено {total_sent} сообщений в Kafka")
    print("Kafka Producer завершен")

if __name__ == '__main__':
    main()
