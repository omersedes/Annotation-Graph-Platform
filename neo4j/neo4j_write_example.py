import time
import sys

from neo4j import GraphDatabase

uri = "bolt://neo4j-ip:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j-user", "neo4j-pword"))

def create_person(tx, name):
    return tx.run("CREATE (a:Person {name:$name}) "
                  "RETURN id(a)", name=name)

def delete_person(tx, name):
    return tx.run("MATCH (n:Person {name:$name})"
                  "DELETE n", name=name)

def create_path(tx, name_1, name_2, count):
    return  tx.run("MATCH (a:Person),(b:Person) "
                   "WHERE a.name = $name_1 AND b.name = $name_2 "
                   "CREATE p = (a)-[r:FRIENDS {count:$count}]->(b) "
                   "RETURN p", name_1 = name_1, name_2 = name_2, count = count).single()

if __name__ == "__main__":
    with driver.session() as session:
        tx = session.begin_transaction()
        create_person(tx, "John")
        create_person(tx, "Fred")
        create_person(tx, "Matt")
        create_path(tx, "John", "Fred","3")
        create_path(tx, "John", "Matt", "10")

        tx.commit()
