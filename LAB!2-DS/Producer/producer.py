import time
import json
import threading
import argparse
import pandas as pd
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092', 'localhost:39092', 'localhost:49092']
producer = Producer({'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP_SERVERS)})

partition = 2

parser = argparse.ArgumentParser()
parser.add_argument("--thread", type=int, default=3, help='setting thread')
parser.add_argument("--delay", type=int, default=1, help='setting delay time')
args = parser.parse_args()
_thread, _delay = args.thread, args.delay

csv_files = {
    'air': r"C:\GIT\Distributed Systems LAB\LAB1_DS\AIR2308.csv",
    'earth': r"C:\GIT\Distributed Systems LAB\LAB1_DS\EARTH2308.csv",
    'water': r"C:\GIT\Distributed Systems LAB\LAB1_DS\WATER2308.csv"
}

KAFKA_TOPICS = {
    'air': 'AIR',
    'earth': 'EARTH',
    'water': 'WATER'
}

def generate_data(source, num):
    csv_file = csv_files[source]
    KAFKA_TOPIC = KAFKA_TOPICS[source]

    df = pd.read_csv(csv_file)
    for index in range(len(df)):
        try:
            partition_id = index % partition

            row = df.iloc[index]
            msg = {}

            for col in df.columns:
                value = row[col]
                if isinstance(value, (int, float)):
                    msg[col] = value
                else:
                    msg[col] = str(value)

            producer.produce(KAFKA_TOPIC, partition=partition_id, value=json.dumps(msg))
            producer.poll(1)

            print(f"Publish message {source.upper()} data: ", msg)
            time.sleep(_delay)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            break


if __name__ == '__main__':
    thread_lst = []
    sources = ['air', 'earth', 'water']

    for i in range(_thread):
        source = sources[i]
        t = threading.Thread(target=generate_data, args=(source, i))
        t.start()
        thread_lst.append(t)

    for t in thread_lst:
        t.join()

    print("Done!")
