'''from kafka import KafkaConsumer
import json
import pydoop.hdfs as hdfs

bootstrap_servers = 'localhost:9092'
topic_name = 'cryptocurrency_data'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: x.decode('utf-8')
)

file_path = 'hdfs://localhost:9000/tp-kafka-flink/currency_data.json'

try:
    with hdfs.open(file_path, 'at') as file:
        for message in consumer:
            currency_data = message.value
            print("Received message:", currency_data)
            
            data_str = json.dumps(currency_data)
            file.write(currency_data)
            file.flush()
            print("Stored data in HDFS successfully")

except Exception as e:
    print("Failed to store data in HDFS:", str(e))

consumer.close()


'''
from kafka import KafkaConsumer
import json
import pydoop.hdfs as hdfs

bootstrap_servers = 'localhost:9092'
topic_name = 'cryptocurrency_data'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: x.decode('utf-8')
)

file_path = 'hdfs://localhost:9000/tp-kafka-flink/currency_data.json'

try:
    with hdfs.open(file_path, 'at') as file:
        for message in consumer:
            currency_data = message.value
            print("Received message:", currency_data)
            
            data_str = json.dumps(currency_data)
            file.write(currency_data)
            file.flush()
            print("Stored data in HDFS successfully")

except Exception as e:
    print("Failed to store data in HDFS:", str(e))

consumer.close()
