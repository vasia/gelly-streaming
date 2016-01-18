Gelly Streaming
===============

An experiemental API for single-pass graph streaming analytics on [Apache Flink](https://flink.apache.org/).

###Graph Stream Types

* `SimpleEdgeStream`: A graph stream where the stream consists solely of edges additions without timestamps.
* `GraphWindowStream`: A stream of discrete graphs, each maintaining the graph state of the edges contained in the respective window. It is created by calling `SimpleEdgeStream.slice()`. The graph slice is keyed by the source or target vertex of the edge stream, so that all edges of a vertex are in the same window.

###Properties and Metrics

* `getVertices`: the stream of vertices.
* `getEdges`: the stream of edges.
* `numberOfVertices`: a continuously improving data stream representing the number of vertices in the graph stream.
* `numberOfEdges`: a continuously improving data stream representing the number of edges in the graph stream, including possible duplicates.
* `getDegrees`: a continuously improving stream of the graph degrees.
* `inDegrees`: a continuously improving stream of the graph in-degrees.
* `outDegrees`: a continuously improving stream of the graph out-degrees.


###Transformations

* `mapEdges`: applies a map function to the each edge value in the graph stream.
* `filterVertices`: applies a filter function to each vertex in the graph stream.
* `filterEdges`: applies a filter function to each edge in the graph stream.
* `distinct`: the stream of disatinct edges in the graph stream.
* `reverse`: a graph stream where edge directions have been reversed.
* `undirected`: adds the opposite-direction edges to the graph stream.
* `union`: merges two graph streams.


###Aggregations

* `aggegate`: Applies an incremental aggregation on a graph stream and returns a stream of aggregation results.
* `globalAggregate`: Applies a global aggregate on the vertex stream.

###Discretization

* `slice`: Discretizes the edge stream into tumbling windows of the specified size.

####Neighborhood Aggregations

* `foldNeighbors`: Performs a neighborhood fold on the graph window stream.
* `reduceOnEdges`: Performs an aggregation on the neighboring edges of each vertex on the graph window stream.


## Graph Streaming Algorithms

* Connected Components
* Bipartiteness Check
* Triangle Count Estimation
* Weighted Matching
* Continuous Degree Aggregate
