from __future__ import print_function
import sys
import time
from neo4j import GraphDatabase
from pyspark import SparkContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#Functions To Write To Neo4j

def write_relationship(tx, pair):
    return tx.run("WITH $pair as pair "
                   "UNWIND pair as p " 
                   "MERGE (a:Tag {tag_name:p[0]}) "
                   "MERGE (b:Tag {tag_name:p[1]}) "
                   "MERGE (a)-[r:IN_THE_SAME_PICTURE]->(b) "
                   "ON CREATE SET r.weight = 1 "
                   "ON MATCH SET r.weight = r.weight + 1", pair = pair)


def RddToNeo4J(iter):
       uri = "bolt://ec2-3-81-114-183.compute-1.amazonaws.com:7687"
       driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j2"))
       with driver.session() as session:
            #for pair in iter:
            for i in range(100):
                    try:
                        tx = session.begin_transaction()
                        for pair in iter:
                            write_relationship(tx, pair)
                        tx.commit()
                        break
                    except:
                        continue
            session.close()

def TagsToPairs(tags):
    pairs = list()
    if tags != []:
           for i in range(len(tags)):
               for j in range(i):
                   if i != j:
                      tag_1 = tags[i]
                      tag_2 = tags[j]
                      pairs.append([tag_1, tag_2])
    return pairs          

if __name__ == "__main__":
        if len(sys.argv) != 3:
           print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
           exit(-1)
    #uri = "bolt://ec2-3-81-114-183.compute-1.amazonaws.com:7687"
    #driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j2"))
    #with driver.session() as session:
        sc = SparkContext(appName="ImageAnnotations")
        sc.setLogLevel("WARN")
        ssc = StreamingContext(sc, 1.5)
         
        zkQuorum, topic = sys.argv[1:]
        kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": zkQuorum})
        lines = kvs.map(lambda x: x[1].encode('ascii', 'ignore'))
        #lines.pprint()
        tags = lines.map(lambda x: (x.split(":")[1])).map(lambda x: (x.split("[")[1])).map(lambda x: (x.split("]")[0])).map(lambda x: (x.split(", ")))
        pairs = tags.map(TagsToPairs)
        
        pairs.foreachRDD(lambda rdd: rdd.foreachPartition(RddToNeo4J))
        #pairs.foreachRDD(lambda rdd: rdd.foreach(RddToNeo4J)) 
        
        ssc.start()
        ssc.awaitTermination()
