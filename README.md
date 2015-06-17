gelly-streaming 
===============

A prototype API for single-pass graph streaming on Apache Flink

##Methods

The main abstraction is a graph stream, which is currently represented as an edge-only stream.
The following methods are currently supported on EdgeOnlyStream:

###Properties and Metrics

* getVertices
* getEdges
* numberOfVertices
* numberOfEdges
* getDegrees
* inDegrees
* outDegrees


###Transformations

* mapEdges
* filterVertices
* filterEdges
* distinct
* reverse
* incidenceSample


###Aggregations

* aggegate
* globalAggregate
* mergeTree


## Graph Streaming Algorithms

* Bipartiteness Check
* Triangle Count Estimation
* Weighted Matching
* Continuous Degree Aggregate
