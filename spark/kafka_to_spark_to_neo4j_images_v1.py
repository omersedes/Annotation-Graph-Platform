from __future__ import print_function
import sys

from neo4j import GraphDatabase
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
                   "MERGE (a)-[r:IN_THE_SAME_PICTURE]-(b) "
                   "ON CREATE SET r.weight = 1 "
                   "ON MATCH SET r.weight = r.weight + 1 "
                   "RETURN id(r)", tag_1 = tag_1, tag_2 = tag_2)


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
                          pairs.append((n[i], n[j]))
              # print(pairs)
    # Writing each tag pair to neo4j           
               if pairs != []: 
                  for p in pairs:
                      write_node(tx, p[0])
                      write_node(tx, p[1])
                      write_edge(tx, p[0], p[1]) 
        tx.commit()    


if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
		exit(-1)
			uri = "bolt://neo4j-ip:7687"
				driver = GraphDatabase.driver(uri, auth=("neo4j-user", "neo4j-pword"))
				with driver.session() as session:
					sc = SparkContext(appName="ImageAnnotations")
					sc.setLogLevel("WARN")
					ssc = StreamingContext(sc, 1)

zkQuorum, topic = sys.argv[1:]

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": zkQuorum})

	lines = kvs.map(lambda x: x[1])
	
	tags = lines.map(lambda x: (x.split(":")[1])).map(lambda x: (x.split("[")[1])).map(lambda x: (x.split("]")[0])).map(lambda x: (x.split(", ")))
	
	tags.foreachRDD(lambda n: RddToNeo4J(n,session))
	
	ssc.start()
	ssc.awaitTermination()
