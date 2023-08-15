from kafka import KafkaProducer
import requests
import json
import time
import pprint
import argparse

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process some arguments.')
parser.add_argument('--bootstrap-servers', nargs='+', help='Kafka broker addresses')
parser.add_argument('--api-key', help='API key')
parser.add_argument('--dep-iata', help='Departure Airport IATA code')
parser.add_argument('--arr_iata', help='Arrival Airport IATA code')
parser.add_argument('--flight_number', help='Flight_number')
parser.add_argument('--airline_iata', help='Airline_iata IATA code')
parser.add_argument('--reg_number', help='Registration number')
parser.add_argument('--flight_iata', help='Flight IATA code')
parser.add_argument('--flag', help='Airline Country ISO 2 code')
parser.add_argument('--minutes', type=int, help='Number of minutes to run')
parser.add_argument('--requests-per-minute', type=int, help='Number of requests per minute')
args = parser.parse_args()

# Kafka broker address
bootstrap_servers = args.bootstrap_servers if args.bootstrap_servers else ['localhost:9092', 'localhost:9093', 'localhost:9094']

# Kafka topic name
topic_name = 'flights'

# Create Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

api_base = 'http://airlabs.co/api/v9/'

params = {
    'api_key': args.api_key if args.api_key else 'a1ede749-110f-49a9-95d3-184d166d8b84',
}

method = 'flights'

if  args.dep_iata:
    params['dep_iata'] =  args.dep_iata

if args.arr_iata:
    params['arr-iata'] = args.arr_iata

if args.flight_number:
    params['flight_number'] = args.flight_number

if args.airline_iata:
    params['airline_iata'] = args.airline_iata

if args.flight_iata:
    params['flight_iata'] = args.flight_iata

if args.reg_number:
    params['reg_number'] = args.reg_number

if args.flag:
    params['flag'] = args.flag

# Define the number of minutes
minutes = args.minutes if args.minutes else 5

# Define the number of requests per minute
requests_per_minute = args.requests_per_minute if args.requests_per_minute else 10

# Calculate the delay between each request
delay = 60 / requests_per_minute

for _ in range(minutes):
    for _ in range(requests_per_minute):
        # Make the API request
        api_result = requests.get(api_base + method, params=params)

        # Get the JSON response
        api_response = api_result.json()

        # Print the response
        pprint.pprint(api_response['response'])

        # Send JSON data to Kafka topic
        producer.send(topic_name, value=api_response)

        # Flush the producer to ensure delivery
        producer.flush()

        # Delay between requests
        time.sleep(delay)

time.sleep(20)

# Close the producer connection
producer.close()
