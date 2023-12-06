'''
import json
from kafka import KafkaProducer
import requests

# Fetch data from CoinGecko API
url = 'https://api.coingecko.com/api/v3/coins/list'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    # Extract relevant data or process as needed before sending to Kafka
    cryptocurrencies = data[:]  # Limit to the first 10 cryptocurrencies for demonstration
    
    # Kafka Producer configuration
    bootstrap_servers = 'localhost:9092'  # Change this to your Kafka broker address
    topic_name = 'cryptocurrency_data'  # Change this to your desired topic name

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for currency in cryptocurrencies:
    try:
        # Produce data to Kafka topic
        producer.send(topic_name, value=currency)
        print("Sent data for {} to Kafka topic '{}' successfully".format(currency['name'], topic_name))
    except Exception as e:
        print("Failed to send data for {} to Kafka topic '{}': {}".format(currency['name'], topic_name, str(e)))

    #for currency in cryptocurrencies:
      #  producer.send(topic_name, value=currency)

 # Close the producer connection
else:
    print('Failed to fetch data:', response.status_code)
#producer.flush()  # Ensure all buffered messages are sent
#producer.close() 
'''
import json
from kafka import KafkaProducer
import requests

# Kafka Producer configuration
bootstrap_servers = 'localhost:9092'  # Change this to your Kafka broker address
topic_name = 'cryptocurrency_data'  # Change this to your desired topic name

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

url = 'https://api.coingecko.com/api/v3/coins/list'
page = 1
per_page = 250  # Maximum allowed per_page value by CoinGecko API

while True:
    params = {'page': page, 'per_page': per_page}
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        cryptocurrencies = data

        for currency in cryptocurrencies:
            try:
                # Produce data to Kafka topic
                producer.send(topic_name, value=currency)
                print("Sent data for {} to Kafka topic '{}' successfully".format(currency['name'], topic_name))
            except Exception as e:
                print("Failed to send data for {} to Kafka topic '{}': {}".format(currency['name'], topic_name, str(e)))

        # Check if there's more data to fetch
        if len(cryptocurrencies) < per_page:
            break  # Exit loop if the returned data is less than per_page (last page)
        else:
            page += 1  # Increment page number for the next request
    else:
        print('Failed to fetch data:', response.status_code)
        break

# Close the producer connection
producer.close()
