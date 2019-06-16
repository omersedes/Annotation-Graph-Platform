from kafka.client import KafkaClient
from kafka import KafkaConsumer

print('starting')
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('price_data_part4',
                         bootstrap_servers=['ip:9092'])
print('messages start')

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
