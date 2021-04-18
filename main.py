from modules import controller

if __name__ == '__main__':

    # config = access_api.Controller()
    # config.get_api_data()
    # config.send_data_to_topic()
    # config.consume_data_from_topic()

    import yaml
    import time
    import json
    import requests

    from datetime import datetime
    from kafka import KafkaProducer
    from kafka import KafkaConsumer
    from IPython.display import clear_output

    with open(r'.\config.yaml', 'r') as config_file:
        yaml_config = yaml.load(config_file)

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    response = requests.get(
        yaml_config['public_url'],
        auth=(
            yaml_config['public_username'],
            yaml_config['public_password']
        ),
        verify=True
    )

    content_api = response.content
    content_json_loaded = json.loads(content_api)
    real_content = content_json_loaded['hits']['hits']
    parsed_content = [content['_source'] for content in real_content]

    broker = yaml_config['broker_url']
    topic = yaml_config['topic_name']
    producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=lambda x:
                             json.dumps(x).encode('utf-8'))

    for row in parsed_content:
        row['date_collected_data'] = now
        producer.send(topic, value=row)
        time.sleep(0.5)

    brokers = [yaml_config['broker_url']]
    topic = yaml_config['topic_name']
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=brokers,
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                             )
    consumer.subscribe(yaml_config['topic_name'])

    for message in consumer:
        print(message.offset, message.value)
