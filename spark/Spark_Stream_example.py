import os
import findspark
findspark.init('/usr/hdp/2.5.6.0-40/spark')
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 pyspark-shell'
sc = SparkContext(appName="PythonSparkStreamingKafka")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc,60)
print('ssc =================== {} {}');
kafkaStream = KafkaUtils.createStream(ssc, 'victoria.com:2181', 'spark-streaming', {'imagetext':1})
print('contexts =================== {} {}');
lines = kafkaStream.map(lambda x: x[1])
lines.pprint()

ssc.start()
ssc.awaitTermination()
