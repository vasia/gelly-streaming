Gelly Streaming
===============

An experiemental API for single-pass graph streaming analytics on [Apache Flink](https://flink.apache.org/).

## A Graph Streaming Model
We implement a light-weight distributed graph streaming model for online processing of graph statistics, improving
aggregates, approximations, one-pass algorithms and graph windows, on **unbounded** graph streams. Our work builds on existing abstractions for stream processing on distributed dataflows, and specifically of Apache Flink.

There are two main abstractions: the `GraphStream` and the `GraphWindowStream`.

The `GraphStream` is the core abstract graph stream representation in our model. A graph stream can be constructed by a given data stream of edges (edge additions). Edges can hold optional state such as weights and reoccurrence of an edge is simply reprocessed. Such an edge stream is fundamental in most evolving graph use cases. For example, consider a dynamic social graph made out of streams of user events that occur in a social network. In that case vertices represent users and edges any possible interaction between them. The graph stream can encapsulate several incremental transformations, property streams, and streaming aggregations such as a global reduce function.

The `GraphWindowStream` forms a stream of discrete graphs, each maintaining the graph state of the edges contained in a time-based window. This is particularly useful to applications that are interested in the most recent or fresh state of the data to apply computations. We call such an operation a graph `slice`. Apart from the existing graph stream transformations and properties, graph windows also support additional aggregations, such as reduce functions on neighbors.

A graph stream **does not maintain the graph structure** internally per se, but rather a summary distributed over
stateful operators in the execution dataflow.

### Graph Stream Types

* `SimpleEdgeStream`: A graph stream where the stream consists solely of edges additions without timestamps.
* `GraphWindowStream`: A stream of discrete graphs, each maintaining the graph state of the edges contained in the respective window. It is created by calling `SimpleEdgeStream.slice()`. The graph slice is keyed by the source or target vertex of the edge stream, so that all edges of a vertex are in the same window.

### Properties and Metrics

* `getVertices`: the stream of vertices.
* `getEdges`: the stream of edges.
* `numberOfVertices`: a continuously improving data stream representing the number of vertices in the graph stream.
* `numberOfEdges`: a continuously improving data stream representing the number of edges in the graph stream, including possible duplicates.
* `getDegrees`: a continuously improving stream of the graph degrees.
* `inDegrees`: a continuously improving stream of the graph in-degrees.
* `outDegrees`: a continuously improving stream of the graph out-degrees.


### Transformations

* `mapEdges`: applies a map function to the each edge value in the graph stream.
* `filterVertices`: applies a filter function to each vertex in the graph stream.
* `filterEdges`: applies a filter function to each edge in the graph stream.
* `distinct`: the stream of disatinct edges in the graph stream.
* `reverse`: a graph stream where edge directions have been reversed.
* `undirected`: adds the opposite-direction edges to the graph stream.
* `union`: merges two graph streams.


### Aggregations

* `aggegate`: Applies an incremental aggregation on a graph stream and returns a stream of aggregation results.
* `globalAggregate`: Applies a global aggregate on the vertex stream.

### Discretization

* `slice`: Discretizes the edge stream into tumbling windows of the specified size.

#### Neighborhood Aggregations

* `foldNeighbors`: Performs a neighborhood fold on the graph window stream.
* `reduceOnEdges`: Performs an aggregation on the neighboring edges of each vertex on the graph window stream.
* `applyOnNeighbors`: Performs a a generic neighborhood aggregation in the graph window stream, where each vertex can produce zero, one or more values from the computation on its neighborhood.


## Graph Streaming Algorithms

* Connected Components
* k-Spanner
* Bipartiteness Check
* Window Triangle Count
* Triangle Count Estimation
* Weighted Matching
* Continuous Degree Aggregate
