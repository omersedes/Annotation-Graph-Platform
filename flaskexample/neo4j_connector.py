from py2neo import Graph
import os 

class neo4jConnector():
    """
    Connector for neo4j database from Spark
    """

    def __init__(self):
        """
        Initialize neo4j connector with my neo4j username and password.
        """
        self.graph = Graph("bolt://ec2-52-90-203-215.compute-1.amazonaws.com:7687",os.environ.get('neo4j'),
                           password=os.environ.get('sq@V$L5#S6DxRDGz'))

if __name__ == '__main__':
    print(neo4jConnector().graph)
