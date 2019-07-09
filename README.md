# Annotation-Graph-Platform

# Introduction

Due to the recent advances in machine learning, object and/or face recognition can be performed rapidly and accurately, at a very large scale. Making decisions and performing analytics based on a large amount of annotations is an important part of the effort around machine learning advances. The Annotation graph platform is a project with the principal aim of drawing meaningful conclusions and insights from annotated images, by taking streams of annotation data and mapping it to a graph database. 

This procedure creates a dynamicly evolving weighted complex network of images annotations. The nodes (or vertices) of this network are the annotations themselves, and an edge is created between two annotations when they are identified in the same image (or frame, if the source is a video). The edge weight is increased when the same pair of annotations are detected in a consequent image. For example, if John and Jill are both present in 10 images, they are represented by two vertices connected by an edge of strength 10.

Once such a graph is created, it can be analyzed to identify clusters of annotations. On the most basic level, the analyst might be interested in finding the list of objects that are most likely to be found in the same image . We can also perform more advanced analytics based on established complex network analysis metrics, as described in the section below.

# Data Source

Detailing Open Images, preformatting of the images from csv files to json files. 

# Overall Pipeline

# Image Annotation Streams As Kafka Producers

# Spark Streaming as Microbatches For Generating Edge Lists

# Neo4j Graph Database For Fast Graph Algorithm Queries

# Instructions

# Complex Network Analysis of the Image Annotation Graph

# Upcoming Features and Improvements
Interfacing with Tensor Flow.


