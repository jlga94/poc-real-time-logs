import json
import os
import argparse
from time import sleep

from kafka import KafkaConsumer, KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print ('Message published successfully.')
    except Exception as ex:
        print ('Exception in publishing message')
        print (ex)

def connect_kafka_producer(kafka_broker):
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[kafka_broker], api_version=(0, 10))
    except Exception as ex:
        print ('Exception while connecting Kafka')
        print (ex)
    finally:
        return _producer

if __name__ == '__main__':
    topic_name = "http_log"
    #kafka_broker = os.getenv('KAFKA_BROKER')
    kafka_broker = "10.15.19.76:9092"
    producer = connect_kafka_producer(kafka_broker)
    while(True):
        sleep(5)
        for file in os.listdir("."):
            if file.endswith(".log"):
                print ("Filename: " + file)
                with open(file,'r') as f:
                    lines = f.readlines()
                    for line in lines:
                        print (line)
                        publish_message(producer, topic_name, 'parsed', line)
                f = open(file,'w')
                f.close()
