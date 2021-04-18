import yaml
import time
import json
import requests

from datetime import datetime
from kafka import KafkaProducer
from kafka import KafkaConsumer


class Controller:

    def __init__(self):
        with open(r'.\config.yaml', 'r') as config_file:
            self.yaml_config = yaml.load(config_file)

        self.now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def get_api_data(self):
        response = requests.get(
            self.yaml_config['public_url'],
            auth=(
                self.yaml_config['public_username'],
                self.yaml_config['public_password']
            ),
            verify=True
        )

        content_api = response.content
        content_json_loaded = json.loads(content_api)
        real_content = content_json_loaded['hits']['hits']
        self.parsed_content = [content['_source'] for content in real_content]

    def send_data_to_topic(self):

        broker = self.yaml_config['broker_url']
        topic = self.yaml_config['topic_name']
        producer = KafkaProducer(bootstrap_servers=[broker],
                                 value_serializer=lambda x:
                                 json.dumps(x).encode('utf-8'))

        for row in self.parsed_content:
            row['date_collected_data'] = self.now
            producer.send(topic, value=row)
            time.sleep(0.5)

    def consume_data_from_topic(self):

        brokers = [self.yaml_config['broker_url']]
        topic = self.yaml_config['topic_name']

        consumer = KafkaConsumer(topic,
                                 bootstrap_servers=brokers,
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                                 )
        consumer.subscribe(self.yaml_config['topic_name'])

        for message in consumer:
            print(message.offset, message.value)
