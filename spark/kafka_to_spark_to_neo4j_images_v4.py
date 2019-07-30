from __future__ import print_function
import sys

from neo4j import GraphDatabase
from pyspark import SparkContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#Functions To Write To Neo4j

def write_relationship(tx, tag_1, tag_2):
    return  tx.run("MERGE (a:Tag {tag_name:$tag_1}) "
                   "MERGE (b:Tag {tag_name:$tag_2}) "
                   "MERGE (a)-[r:IN_THE_SAME_PICTURE]->(b) "
                   "ON CREATE SET r.weight = 1 "
                   "ON MATCH SET r.weight = r.weight + 1", tag_1 = tag_1, tag_2 = tag_2)


def RddToNeo4J(rdd, session):
    if (rdd.count() != 0):
        nodes = rdd.toLocalIterator()
        tx = session.begin_transaction() 
    # Counting Unique Pair Tags        
        if nodes != []:
           for n in nodes:
               pairs = list()
               for i in range(len(n)): 
                   for j in range(i): 
                       if i != j:
                          tag_1 = n[i]
                          tag_2 = n[j]
                          #print(tag_1, tag_2)               
                          write_relationship(tx, tag_1, tag_2)
        tx.commit()    


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)
    uri = "bolt://ec2-3-81-114-183.compute-1.amazonaws.com:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j2"))
    with driver.session() as session:
        sc = SparkContext(appName="ImageAnnotations")
        sc.setLogLevel("WARN")
        ssc = StreamingContext(sc, 1)
         
        zkQuorum, topic = sys.argv[1:]
        kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": zkQuorum})
        lines = kvs.map(lambda x: x[1].encode('ascii', 'ignore'))
        #lines.pprint()
        tags = lines.map(lambda x: (x.split(":")[1])).map(lambda x: (x.split("[")[1])).map(lambda x: (x.split("]")[0])).map(lambda x: (x.split(", ")))
        tags.foreachRDD(lambda n: RddToNeo4J(n, session))
        #tags.pprint()
        ssc.start()
        ssc.awaitTermination()
