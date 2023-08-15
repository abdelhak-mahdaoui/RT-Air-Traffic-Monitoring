from kafka import KafkaProducer
import requests
import json
import time
import pprint
from kafka import KafkaConsumer

# Kafka broker address
bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

# Kafka topic name
topic_name = 'flight_info'

# Create Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Create Kafka consumer instance
consumer = KafkaConsumer("flights",
                        bootstrap_servers=bootstrap_servers,
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

api_base = 'http://airlabs.co/api/v9/'

params = {8
            'api_key': 'a1ede749-110f-49a9-95d3-184d166db84',
        }

method = 'flight'                     

# Continuously read and process messages from Kafka
for message in consumer:
    message_value = message.value
    for element in message_value['response']:
        # Get the deserialized JSON data
        data = element

        flight_iata = data['flight_iata']

        if  flight_iata:
            params['flight_iata'] =  flight_iata

        # Make the API request
        api_result = requests.get(api_base + method, params=params)

        # Get the JSON response
        api_response = api_result.json()

        # Print the response
        pprint.pprint(api_response['response'])

        # Send JSON data to Kafka topic
        producer.send(topic_name, value=api_response['response'])

        # Flush the producer to ensure delivery
        producer.flush()


# Close the producer connection
producer.close()

# Close the consumer connection
consumer.close()

                       

