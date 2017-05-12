/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 *
 * Represents a graph stream where the stream consists solely of {@link org.apache.flink.graph.Edge edges}.
 * <p>
 *
 * @see org.apache.flink.graph.Edge
 *
 * @param <K> the key type for edge and vertex identifiers.
 * @param <EV> the value type for edges.
 */
@SuppressWarnings("serial")
public class SimpleEdgeStream<K, EV> extends GraphStream<K, NullValue, EV> {

	private final StreamExecutionEnvironment context;
	private final DataStream<Edge<K, EV>> edges;

	/**
	 * Creates a graph from an edge stream.
	 * The time characteristic is set to ingestion time  by default.
	 * 
	 * @see {@link org.apache.flink.streaming.api.TimeCharacteristic}
	 *
	 * @param edges a DataStream of edges.
	 * @param context the flink execution environment.
	 */
	public SimpleEdgeStream(DataStream<Edge<K, EV>> edges, StreamExecutionEnvironment context) {
		context.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		this.edges = edges;
		this.context = context;
	}

	/**
	 * Creates a graph from an edge stream operating in event time specified by timeExtractor .
	 * 
	 * The time characteristic is set to event time.
	 * 
	 * @see {@link org.apache.flink.streaming.api.TimeCharacteristic}
	 * 
	 * @param edges a DataStream of edges.
	 * @param timeExtractor the timestamp extractor.
	 * @param context the execution environment.
     */
	public SimpleEdgeStream(DataStream<Edge<K, EV>> edges, AscendingTimestampExtractor<Edge<K,EV>> timeExtractor, StreamExecutionEnvironment context) {
		context.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		this.edges = edges.assignTimestampsAndWatermarks(timeExtractor);
		this.context = context;
	}

	/**
	 * Applies an incremental aggregation on a graphstream and returns a stream of aggregation results 
	 * 
	 * @param summaryAggregation
	 * @param <S>
	 * @param <T>
     * @return
     */
	public <S extends Serializable, T> DataStream<T> aggregate(SummaryAggregation<K,EV,S,T> summaryAggregation) {
		return summaryAggregation.run(getEdges());//FIXME
	}
	
	/**
	 * @return the flink streaming execution environment.
	 */
	@Override
	public StreamExecutionEnvironment getContext() {
		return this.context;
	}

	/**
	 * @return the vertex DataStream.
	 */
	@Override
	public DataStream<Vertex<K, NullValue>> getVertices() {
		return this.edges
			.flatMap(new EmitSrcAndTarget<K, EV>())
			.keyBy(0)
			.filter(new FilterDistinctVertices<K>());
	}

	/**
	 * Discretizes the edge stream into tumbling windows of the specified size.
	 * <p>
	 * The edge stream is partitioned so that all neighbors of a vertex belong to the same partition.
	 * The KeyedStream is then windowed into tumbling time windows.
	 * <p>
	 * By default, each vertex is grouped with its outgoing edges.
	 * Use {@link #slice(Time, EdgeDirection)} to manually set the edge direction grouping.
	 * 
	 * @param size the size of the window
	 * @return a GraphWindowStream of the specified size 
	 */
	public SnapshotStream<K, EV> slice(Time size) {
		return slice(size, EdgeDirection.OUT);
	}

	/**
	 * Discretizes the edge stream into tumbling windows of the specified size.
	 * <p>
	 * The edge stream is partitioned so that all neighbors of a vertex belong to the same partition.
	 * The KeyedStream is then windowed into tumbling time windows.
	 * 
	 * @param size the size of the window
	 * @param direction the EdgeDirection to key by
	 * @return a GraphWindowStream of the specified size, keyed by
	 */
	public SnapshotStream<K, EV> slice(Time size, EdgeDirection direction)
		throws IllegalArgumentException {

		switch (direction) {
		case IN:
			return new SnapshotStream<K, EV>(
				this.reverse().getEdges().keyBy(new NeighborKeySelector<K, EV>(0)).timeWindow(size));
		case OUT:
			return new SnapshotStream<K, EV>(
				getEdges().keyBy(new NeighborKeySelector<K, EV>(0)).timeWindow(size));
		case ALL:
			getEdges().keyBy(0).timeWindow(size);
			return new SnapshotStream<K, EV>(
				this.undirected().getEdges().keyBy(
					new NeighborKeySelector<K, EV>(0)).timeWindow(size));
		default:
			throw new IllegalArgumentException("Illegal edge direction");
		}
	}

	private static final class NeighborKeySelector<K, EV> implements KeySelector<Edge<K, EV>, K> {
		private final int key;

		public NeighborKeySelector(int k) {
			this.key = k;
		}

		public K getKey(Edge<K, EV> edge) throws Exception {
			return edge.getField(key);
		}
	}

	private static final class EmitSrcAndTarget<K, EV>
			implements FlatMapFunction<Edge<K, EV>, Vertex<K, NullValue>> {
		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, NullValue>> out) throws Exception {
			out.collect(new Vertex<>(edge.getSource(), NullValue.getInstance()));
			out.collect(new Vertex<>(edge.getTarget(), NullValue.getInstance()));
		}
	}

	private static final class FilterDistinctVertices<K>
			implements FilterFunction<Vertex<K, NullValue>> {
		Set<K> keys = new HashSet<>();

		@Override
		public boolean filter(Vertex<K, NullValue> vertex) throws Exception {
			if (!keys.contains(vertex.getId())) {
				keys.add(vertex.getId());
				return true;
			}
			return false;
		}
	}

	/**
	 * @return the edge DataStream.
	 */
	public DataStream<Edge<K, EV>> getEdges() {
		return this.edges;
	}

	/**
	 * Apply a function to the attribute of each edge in the graph stream.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph stream.
	 */
	public <NV> SimpleEdgeStream<K, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper) {
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);
		DataStream<Edge<K, NV>> mappedEdges = edges.map(new ApplyMapperToEdgeWithType<>(mapper,
				keyType));
		return new SimpleEdgeStream<>(mappedEdges, this.context);
	}

	private static final class ApplyMapperToEdgeWithType<K, EV, NV>
			implements MapFunction<Edge<K, EV>, Edge<K, NV>>, ResultTypeQueryable<Edge<K, NV>> {
		private MapFunction<Edge<K, EV>, NV> innerMapper;
		private transient TypeInformation<K> keyType;

		public ApplyMapperToEdgeWithType(MapFunction<Edge<K, EV>, NV> theMapper, TypeInformation<K> keyType) {
			this.innerMapper = theMapper;
			this.keyType = keyType;
		}

		public Edge<K, NV> map(Edge<K, EV> edge) throws Exception {
			return new Edge<>(edge.getSource(), edge.getTarget(), innerMapper.map(edge));
		}

		@SuppressWarnings("unchecked")
		@Override
		public TypeInformation<Edge<K, NV>> getProducedType() {
			TypeInformation<NV> valueType = TypeExtractor
					.createTypeInfo(MapFunction.class, innerMapper.getClass(), 1, null, null);

			TypeInformation<?> returnType = new TupleTypeInfo<>(Edge.class, keyType, keyType, valueType);
			return (TypeInformation<Edge<K, NV>>) returnType;
		}
	}

	/**
	 * Apply a filter to each vertex in the graph stream
	 * Since this is an edge-only stream, the vertex filter can only access the key of vertices
	 *
	 * @param filter the filter function to apply.
	 * @return the filtered graph stream.
	 */
	@Override
	public SimpleEdgeStream<K, EV> filterVertices(FilterFunction<Vertex<K, NullValue>> filter) {
		DataStream<Edge<K, EV>> remainingEdges = this.edges
				.filter(new ApplyVertexFilterToEdges<K, EV>(filter));

		return new SimpleEdgeStream<>(remainingEdges, this.context);
	}

	private static final class ApplyVertexFilterToEdges<K, EV>
			implements FilterFunction<Edge<K, EV>> {
		private FilterFunction<Vertex<K, NullValue>> vertexFilter;

		public ApplyVertexFilterToEdges(FilterFunction<Vertex<K, NullValue>> vertexFilter) {
			this.vertexFilter = vertexFilter;
		}

		@Override
		public boolean filter(Edge<K, EV> edge) throws Exception {
			boolean sourceVertexKept = vertexFilter.filter(new Vertex<>(edge.getSource(),
					NullValue.getInstance()));
			boolean targetVertexKept = vertexFilter.filter(new Vertex<>(edge.getTarget(),
					NullValue.getInstance()));

			return sourceVertexKept && targetVertexKept;
		}
	}

	/**
	 * Apply a filter to each edge in the graph stream
	 *
	 * @param filter the filter function to apply.
	 * @return the filtered graph stream.
	 */
	@Override
	public SimpleEdgeStream<K, EV> filterEdges(FilterFunction<Edge<K, EV>> filter) {
		DataStream<Edge<K, EV>> remainingEdges = this.edges.filter(filter);
		return new SimpleEdgeStream<>(remainingEdges, this.context);
	}

	/**
	 * Removes the duplicate edges by storing a neighborhood set for each vertex
	 *
	 * @return a graph stream with no duplicate edges
	 */
	@Override
	public SimpleEdgeStream<K, EV> distinct() {
		DataStream<Edge<K, EV>> edgeStream = this.edges
				.keyBy(0)
				.flatMap(new DistinctEdgeMapper<K, EV>());

		return new SimpleEdgeStream<>(edgeStream, this.getContext());
	}

	private static final class DistinctEdgeMapper<K, EV> implements FlatMapFunction<Edge<K, EV>, Edge<K, EV>> {
		private final Set<K> neighbors;

		public DistinctEdgeMapper() {
			this.neighbors = new HashSet<>();
		}

		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out) throws Exception {
			if (!neighbors.contains(edge.getTarget())) {
				neighbors.add(edge.getTarget());
				out.collect(edge);
			}
		}
	}

	/**
	 * @return a graph stream with the edge directions reversed
	 */
	public SimpleEdgeStream<K, EV> reverse() {
		return new SimpleEdgeStream<>(this.edges.map(new ReverseEdgeMapper<K, EV>()), this.getContext());
	}

	private static final class ReverseEdgeMapper<K, EV> implements MapFunction<Edge<K, EV>, Edge<K, EV>> {
		@Override
		public Edge<K, EV> map(Edge<K, EV> edge) throws Exception {
			return edge.reverse();
		}
	}

	/**
	 * @param graph the streamed graph to union with
	 * @return a streamed graph where the two edge streams are merged
	 */
	public SimpleEdgeStream<K, EV> union(SimpleEdgeStream<K, EV> graph) {
		return new SimpleEdgeStream<>(this.edges.union(graph.getEdges()), this.getContext());
	}

	/**
	 * @return a graph stream where edges are undirected
	 */
	public SimpleEdgeStream<K, EV> undirected() {
		DataStream<Edge<K, EV>> reversedEdges = this.edges.flatMap(new UndirectEdges<K, EV>());
		return new SimpleEdgeStream<>(reversedEdges, context);
	}

	private static final class UndirectEdges<K, EV>	implements FlatMapFunction<Edge<K,EV>, Edge<K,EV>> {
		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Edge<K, EV>> out) {
			out.collect(edge);
			out.collect(edge.reverse());
		}
	};

	/**
	 * @return a continuously improving data stream representing the number of vertices in the streamed graph
	 */
	public DataStream<Long> numberOfVertices() {
		return this.globalAggregate(new DegreeTypeSeparator<K, EV>(true, true),
				new VertexCountMapper<K>(), true);
	}

	private static final class VertexCountMapper<K> implements FlatMapFunction<Vertex<K, Long>, Long> {
		private Set<K> vertices;

		public VertexCountMapper() {
			this.vertices = new HashSet<>();
		}

		@Override
		public void flatMap(Vertex<K, Long> vertex, Collector<Long> out) throws Exception {
			vertices.add(vertex.getId());
			out.collect((long) vertices.size());
		}
	}

	/**
	 * @return a data stream representing the number of all edges in the streamed graph, including possible duplicates
	 */
	public DataStream<Long> numberOfEdges() {
		return this.edges.map(new TotalEdgeCountMapper<K, EV>()).setParallelism(1);
	}

	private static final class TotalEdgeCountMapper<K, EV> implements MapFunction<Edge<K, EV>, Long> {
		private long edgeCount;

		public TotalEdgeCountMapper() {
			edgeCount = 0;
		}

		@Override
		public Long map(Edge<K, EV> edge) throws Exception {
			edgeCount++;
			return edgeCount;
		}
	}

	/**
	 * Get the degree stream
	 *
	 * @return a stream of vertices, with the degree as the vertex value
	 * @throws Exception
	 */
	@Override
	public DataStream<Vertex<K, Long>> getDegrees() throws Exception {
		return this.aggregate(new DegreeTypeSeparator<K, EV>(true, true),
				new DegreeMapFunction<K>());
	}

	/**
	 * Get the in-degree stream
	 *
	 * @return a stream of vertices, with the in-degree as the vertex value
	 * @throws Exception
	 */
	public DataStream<Vertex<K, Long>> getInDegrees() throws Exception {
		return this.aggregate(new DegreeTypeSeparator<K, EV>(true, false),
				new DegreeMapFunction<K>());
	}

	/**
	 * Get the out-degree stream
	 *
	 * @return a stream of vertices, with the out-degree as the vertex value
	 * @throws Exception
	 */
	public DataStream<Vertex<K, Long>> getOutDegrees() throws Exception {
		return this.aggregate(new DegreeTypeSeparator<K, EV>(false, true),
				new DegreeMapFunction<K>());
	}

	private static final class DegreeTypeSeparator <K, EV>
			implements FlatMapFunction<Edge<K, EV>, Vertex<K, Long>> {
		private final boolean collectIn;
		private final boolean collectOut;

		public DegreeTypeSeparator(boolean collectIn, boolean collectOut) {
			this.collectIn = collectIn;
			this.collectOut = collectOut;
		}

		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, Long>> out) throws Exception {
			if (collectOut) {
				out.collect(new Vertex<>(edge.getSource(), 1L));
			}
			if (collectIn) {
				out.collect(new Vertex<>(edge.getTarget(), 1L));
			}
		}
	}

	private static final class DegreeMapFunction <K>
			implements MapFunction<Vertex<K, Long>, Vertex<K, Long>> {
		private final Map<K, Long> localDegrees;

		public DegreeMapFunction() {
			localDegrees = new HashMap<>();
		}

		@Override
		public Vertex<K, Long> map(Vertex<K, Long> degree) throws Exception {
			K key = degree.getId();
			if (!localDegrees.containsKey(key)) {
				localDegrees.put(key, 0L);
			}
			localDegrees.put(key, localDegrees.get(key) + degree.getValue());
			return new Vertex<>(key, localDegrees.get(key));
		}
	}

	/**
	 * The aggregate function splits the edge stream up into a vertex stream and applies
	 * a mapper on the resulting vertices
	 *
	 * @param edgeMapper the mapper that converts the edge stream to a vertex stream
	 * @param vertexMapper the mapper that aggregates vertex values
	 * @param <VV> the vertex value used
	 * @return a stream of vertices with the aggregated vertex value
	 */
	public <VV> DataStream<Vertex<K, VV>> aggregate(FlatMapFunction<Edge<K, EV>, Vertex<K, VV>> edgeMapper,
			MapFunction<Vertex<K, VV>, Vertex<K, VV>> vertexMapper) {
		return this.edges.flatMap(edgeMapper)
				.keyBy(0)
				.map(vertexMapper);
	}

	/**
	 * Returns a global aggregate on the previously split vertex stream
	 *
	 * @param edgeMapper the mapper that converts the edge stream to a vertex stream
	 * @param vertexMapper the mapper that aggregates vertex values
	 * @param collectUpdates boolean specifying whether the aggregate should only be collected when there is an update
	 * @param <VV> the return value type
	 * @return a stream of the aggregated values
	 */
	public <VV> DataStream<VV> globalAggregate(FlatMapFunction<Edge<K, EV>, Vertex<K, VV>> edgeMapper,
			FlatMapFunction<Vertex<K, VV>, VV> vertexMapper, boolean collectUpdates) {

		DataStream<VV> result = this.edges.flatMap(edgeMapper)
				.setParallelism(1)
				.flatMap(vertexMapper)
				.setParallelism(1);

		if (collectUpdates) {
			result = result.flatMap(new GlobalAggregateMapper<VV>())
					.setParallelism(1);
		}

		return result;
	}

	//TODO: write tests
	/**
	 * Builds the neighborhood state by creating adjacency lists.
	 * Neighborhoods are currently built using a TreeSet.
	 *
	 * @param directed if true, only the out-neighbors will be stored
	 *                 otherwise both directions are considered
	 * @return a stream of Tuple3, where the first 2 fields identify the edge processed
	 * and the third field is the adjacency list that was updated by processing this edge.
	 */
	public DataStream<Tuple3<K, K, TreeSet<K>>> buildNeighborhood(boolean directed) {

		DataStream<Edge<K, EV>> edges = this.getEdges();
		if (!directed) {
			edges = this.undirected().getEdges();
		}
		return edges.keyBy(0).flatMap(new BuildNeighborhoods<K, EV>());
	}

	private static final class BuildNeighborhoods<K, EV> implements FlatMapFunction<Edge<K, EV>, Tuple3<K, K, TreeSet<K>>> {

		Map<K, TreeSet<K>> neighborhoods = new HashMap<>();
		Tuple3<K, K, TreeSet<K>> outTuple = new Tuple3<>();

		public void flatMap(Edge<K, EV> e, Collector<Tuple3<K, K, TreeSet<K>>> out) {
			TreeSet<K> t;
			if (neighborhoods.containsKey(e.getSource())) {
				t = neighborhoods.get(e.getSource());
			} else {
				t = new TreeSet<>();
			}
			t.add(e.getTarget());
			neighborhoods.put(e.getSource(), t);

			outTuple.setField(e.getSource(), 0);
			outTuple.setField(e.getTarget(), 1);
			outTuple.setField(t, 2);
			out.collect(outTuple);
		}
	}

	private static final class GlobalAggregateMapper<VV> implements FlatMapFunction<VV, VV> {
		VV previousValue;

		public GlobalAggregateMapper() {
			previousValue = null;
		}

		@Override
		public void flatMap(VV vv, Collector<VV> out) throws Exception {
			if (!vv.equals(previousValue)) {
				previousValue = vv;
				out.collect(vv);
			}
		}
	}
}
