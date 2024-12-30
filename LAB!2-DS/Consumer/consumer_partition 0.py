import time
import json
import csv
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
import argparse
import os
import threading

KAFKA_BOOTSTRAP_SERVERS = ['localhost:39092', 'localhost:49092', 'localhost:29092']
KAFKA_TOPICS = ['AIR', 'EARTH', 'WATER']

# Cấu hình Kafka Consumer
consumer_config = {
    'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP_SERVERS),
    'group.id': 'consumer_group',
    'auto.offset.reset': 'earliest'
}

csv_files = {
    'AIR': r"C:\GIT\Distributed Systems LAB\LAB1_DS\Partition 0\AIR_Consumer_0.csv",
    'EARTH': r"C:\GIT\Distributed Systems LAB\LAB1_DS\Partition 0\EARTH_Consumer_0.csv",
    'WATER': r"C:\GIT\Distributed Systems LAB\LAB1_DS\Partition 0\WATER_Consumer_0.csv"
}

def save_data_to_csv(topic, data):
    csv_file = csv_files.get(topic)
    if csv_file:
        file_exists = os.path.exists(csv_file)
        with open(csv_file, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists or f.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
        print(f"Saved data to {topic} file: {data}")
    else:
        print(f"Topic {topic} has no corresponding file!")

def consume_data():
    consumer = Consumer(consumer_config)
    partitions = [TopicPartition(topic, 0) for topic in KAFKA_TOPICS]
    consumer.assign(partitions)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition {msg.partition()} reached at offset {msg.offset()}")
                else:
                    raise KafkaException(msg.error())
            else:
                if msg.partition() == 0:
                    topic = msg.topic()
                    try:
                        message_value = json.loads(msg.value().decode('utf-8'))
                        save_data_to_csv(topic, message_value)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON: {e}")
    except KeyboardInterrupt:
        print("Consumer interrupted.")
    finally:
        consumer.close()

def start_consumer_threads(num_threads):
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=consume_data)
        thread.start()
        threads.append(thread)
        print(f"Started consumer thread {i + 1}")
    for thread in threads:
        thread.join()

def clear_csv_files():
    for csv_file in csv_files.values():
        open(csv_file, 'w').close()
        print(f"Cleared data in {csv_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--thread", type=int, default=1, help='Number of consumer threads')
    args = parser.parse_args()

    clear_csv_files()

    start_consumer_threads(args.thread)
    print("All consumer threads stopped.")
