import time
import json
import sys

from neo4j import GraphDatabase

uri = "bolt://neo4j-ip:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j-user", "neo4j-pword"))


def print_cluster_sizes(tx):
    result = tx.run("CALL algo.unionFind.stream('Tag', 'IN_THE_SAME_PICTURE', {weightProperty:'weight', defaultValue:0.0, threshold:20.0, concurrency: 1}) "
                    "YIELD nodeId, setId "
                    "RETURN setId, count(*) as size_of_component "
                    "ORDER BY size_of_component DESC LIMIT 5 ")
    for record in result:
        print("cluster {} has size {}".format(record["setId"] ,record["size_of_component"]))

def print_number_of_clusters(tx):
    result = tx.run("CALL algo.unionFind.stream('Tag', 'IN_THE_SAME_PICTURE', {weightProperty:'weight', defaultValue:0.0, threshold:20.0, concurrency: 1}) "
                    "YIELD nodeId, setId "
                    "RETURN count(distinct setId) as count_of_components")
    for record in result:
        print("The graph has {} connected clusters".format(record["count_of_components"]))

if __name__ == "__main__":
    with driver.session() as session:
        while True:
          tx = session.begin_transaction()
          print("-------------------------")
          print(time.strftime("%Y-%m-%d %H:%M:%S"))
          print_number_of_clusters(tx)
          print_cluster_sizes(tx)
          tx.commit()
          time.sleep(2.0)
