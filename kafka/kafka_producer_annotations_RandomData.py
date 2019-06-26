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

    def produce_msgs(self, source_symbol):
        image_id = 0
        msg_cnt = 0
        while msg_cnt <= 100000:
            image_id += 1
            tag_id = []
            number_of_tags = random.randint(0, 20)
            for n in range(number_of_tags):
                tag_id.append(str(random.randint(1, 500)))
            message_info = {str(image_id):tag_id}
            print(message_info)
            time.sleep(0.1)
            self.producer.send('image_annotation_data', message_info)
            msg_cnt += 1
            #print(msg_cnt)

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)
