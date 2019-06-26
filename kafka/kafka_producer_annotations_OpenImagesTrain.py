import random
import time
import json
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer

class Producer(object):

    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers=addr, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        #self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self, source_symbol):
        msg_cnt = 0
        with open('train-annotations.json') as json_file:
            image_annotations = json.load(json_file)
            for image_id, tags in image_annotations.items():
                image_info = {image_id: tags}

                time.sleep(0.01)
                self.producer.send('image_annotation_data', image_info)
                msg_cnt += 1
                print(image_info, msg_cnt)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
