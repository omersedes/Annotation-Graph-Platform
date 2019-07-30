from __future__ import print_function
import sys

import pyspark.sql.functions as func

from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import lit

from pyspark import SparkContext

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#Functions To Write To Neo4j
def write_node(tx, tag_name):
    return tx.run("MERGE (a:Tag {tag_name:$tag_name}) "
                  "RETURN id(a)", tag_name=tag_name)

def write_edge(tx, tag_1, tag_2):
    return  tx.run("MATCH (a:Tag),(b:Tag) "
                   "WHERE a.tag_name = $tag_1 AND b.tag_name = $tag_2 "
                   "MERGE p = (a)-[r:IN_THE_SAME_PICTURE]->(b) "
                   "RETURN p", tag_1 = tag_1, tag_2 = tag_2)


def RddToNeo4J(rdd, session):
    if (rdd.count() != 0):
        nodes = rdd.collect()

    # Counting Unique Pair Tags        
        if nodes != []:
           for n in range(len(nodes)):
               pairs = list()
               for i in range(len(nodes[n])): 
                   for j in range(i): 
                       #if i != j:
                       pairs.append((nodes[n][i], nodes[n][j]))
		   
    # Writing each tag pair to neo4j           
               if pairs != []: 
                  for p in pairs:
                      tx = session.begin_transaction()
                      tag_1 = p[0]
                      tag_2 = p[1]
                      write_node(tx, tag_1)
                      write_node(tx, tag_2)
                      write_edge(tx, tag_1, tag_2) 
                      tx.commit()


if __name__ == "__main__":

   #Start neo4j connection
   uri = "bolt://ec2-3-81-158-41.compute-1.amazonaws.com:7687"
   driver = GraphDatabase.driver(uri, auth=("neo4j", "i-084ae161961b5aa52"))
   with driver.session() as session: 
        sc = SparkContext(appName="ImageAnnotations")
        sc.setLogLevel("WARN")
        ssc = StreamingContext(sc, 1)
        sqlContext = SQLContext(sc)	

		#Read Kafka Stream
        kvs = KafkaUtils.createDirectStream(ssc, ['image_annotation_data'], {"metadata.broker.list": "ec2-3-218-193-89.compute-1.amazonaws.com:9092,ec2-100-25-53-206.compute-1.amazonaws.com:9092,ec2-3-219-68-65.compute-1.amazonaws.com:9092"})
        lines = kvs.map(lambda x: x[1])
		
		
		#Extract Tags From Kafka Messages
        tags = lines.map(lambda x: (x.split(":")[1])).map(lambda x: (x.split("[")[1])).map(lambda x: (x.split("]")[0])).map(lambda x: (x.split(", ")))
        tags.pprint()
		
		#Calculate Pairs of Tags and Write to neo4j DB
        tags.foreachRDD(lambda n: RddToNeo4J(n,session)) 
    
        ssc.start()
        ssc.awaitTermination()
