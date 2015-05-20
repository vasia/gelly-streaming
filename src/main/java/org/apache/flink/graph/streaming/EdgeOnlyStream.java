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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.RichWindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * Represents a streamed graph where the stream consisting solely of {@link org.apache.flink.graph.Edge edges}
 *
 * @see org.apache.flink.graph.Edge
 *
 * @param <K> the key type for edge and vertex identifiers.
 * @param <EV> the value type for edges.
 */
public class EdgeOnlyStream<K, EV> {

	private final StreamExecutionEnvironment context;
	private final DataStream<Edge<K, EV>> edges;

	/**
	 * Creates a graph from an edge stream
	 *
	 * @param edges a DataStream of edges.
	 * @param context the flink execution environment.
	 */
	public EdgeOnlyStream(DataStream<Edge<K, EV>> edges, StreamExecutionEnvironment context) {
		this.edges = edges;
		this.context = context;
	}

	/**
	 * @return the flink streaming execution environment.
	 */
	public StreamExecutionEnvironment getContext() {
		return this.context;
	}

	/**
	 * @return the vertex DataStream.
	 */
	public DataStream<Vertex<K, NullValue>> getVertices() {
		return this.edges
				.flatMap(new EmitSrcAndTarget<K, EV>())
				.groupBy(0)
				.filter(new FilterDistinctVertices<K>());
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
	public <NV> EdgeOnlyStream<K, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper) {
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);
		DataStream<Edge<K, NV>> mappedEdges = edges.map(new ApplyMapperToEdgeWithType<>(mapper,
				keyType));
		return new EdgeOnlyStream<>(mappedEdges, this.context);
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
			@SuppressWarnings("rawtypes")
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
	public EdgeOnlyStream<K, EV> filterVertices(FilterFunction<Vertex<K, NullValue>> filter) {
		DataStream<Edge<K, EV>> remainingEdges = this.edges
				.filter(new ApplyVertexFilterToEdges<K, EV>(filter));

		return new EdgeOnlyStream<>(remainingEdges, this.context);
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
	public EdgeOnlyStream<K, EV> filterEdges(FilterFunction<Edge<K, EV>> filter) {
		DataStream<Edge<K, EV>> remainingEdges = this.edges.filter(filter);
		return new EdgeOnlyStream<>(remainingEdges, this.context);
	}

	/**
	 * Removes the duplicate edges by storing a neighborhood set for each vertex
	 *
	 * @return a graph stream with no duplicate edges
	 */
	public EdgeOnlyStream<K, EV> distinct() {
		DataStream<Edge<K, EV>> edgeStream = this.edges
				.groupBy(0)
				.flatMap(new DistinctEdgeMapper<K, EV>());

		return new EdgeOnlyStream<>(edgeStream, this.getContext());
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
	 * @return a continuously improving data stream representing the number of vertices in the streamed graph
	 */
	public DataStream<Long> numberOfVertices() {
		return this.globalAggregate(new DegreeTypeSeparator<K, EV>(true, true),
				new VertexCountMapper<K>(), true);
	}

	private static final class VertexCountMapper<K> implements MapFunction<Vertex<K, Long>, Long> {
		private Set<K> vertices;

		public VertexCountMapper() {
			this.vertices = new HashSet<>();
		}

		@Override
		public Long map(Vertex<K, Long> vertex) throws Exception {
			vertices.add(vertex.getId());
			return (long) vertices.size();
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
				.groupBy(0)
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
			MapFunction<Vertex<K, VV>, VV> vertexMapper, boolean collectUpdates) {

		DataStream<VV> result = this.edges.flatMap(edgeMapper)
				.setParallelism(1)
				.map(vertexMapper)
				.setParallelism(1);

		if (collectUpdates) {
			result = result.flatMap(new GlobalAggregateMapper<VV>())
					.setParallelism(1);
		}

		return result;
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

	/**
	 * Aggregates the results of a distributed mapper in a merge-tree structure
	 *
	 * @param initMapper the map function that transforms edges into the intermediate data type
	 * @param treeMapper the map function that performs the aggregation between the data elements
	 * @param windowSize the size of the window to use before the first level
	 * @param <T> the inner data type
	 * @return an aggregated stream of the given data type
	 */
	public <T> DataStream<T> mergeTree(FlatMapFunction<Edge<K, EV>, T> initMapper,
			MapFunction<T, T> treeMapper, int windowSize) {
		int dop = this.context.getParallelism();
		int levels = (int) (Math.log(dop) / Math.log(2));

		DataStream<Tuple2<Integer, T>> chainedStream = this.edges
				.flatMap(new MergeTreeWrapperMapper<>(initMapper));


		for (int i = 0; i < levels; ++i) {
			chainedStream = chainedStream
					.window(Count.of(windowSize / (int) Math.pow(10, i)))
					.mapWindow(new MergeTreeWindowMapper<>(treeMapper))
					.flatten();

			if (i < levels - 1) {
				chainedStream = chainedStream.groupBy(new MergeTreeKeySelector<T>(i));
			}
		}

		return chainedStream.map(new MergeTreeProjectionMapper<T>());
	}

	private static final class MergeTreeWrapperMapper<K, EV, T>
			extends RichFlatMapFunction<Edge<K, EV>, Tuple2<Integer, T>>
			implements ResultTypeQueryable<Tuple2<Integer, T>> {
		private final FlatMapFunction<Edge<K, EV>, T> initMapper;

		public MergeTreeWrapperMapper(FlatMapFunction<Edge<K, EV>, T> initMapper) {
			this.initMapper = initMapper;
		}

		@Override
		public void flatMap(Edge<K, EV> edge, final Collector<Tuple2<Integer, T>> out) throws Exception {
			initMapper.flatMap(edge, new Collector<T>() {
				@Override
				public void collect(T t) {
					out.collect(new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), t));
				}

				@Override
				public void close() {}
			});
		}

		@Override
		public TypeInformation<Tuple2<Integer, T>> getProducedType() {
			TypeInformation<Integer> keyType = TypeExtractor.getForClass(Integer.class);
			TypeInformation<T> innerType = TypeExtractor.createTypeInfo(FlatMapFunction.class,
					initMapper.getClass(), 1, null, null);

			return new TupleTypeInfo<>(keyType, innerType);
		}
	}

	private static final class MergeTreeWindowMapper<T>
			extends RichWindowMapFunction<Tuple2<Integer, T>, Tuple2<Integer, T>> {
		private final MapFunction<T, T> treeMapper;

		public MergeTreeWindowMapper(MapFunction<T, T> treeMapper) {
			this.treeMapper = treeMapper;
		}

		@Override
		public void mapWindow(Iterable<Tuple2<Integer, T>> iterable,
				Collector<Tuple2<Integer, T>> out) throws Exception {
			T t = null;
			for (Tuple2<Integer, T> entry : iterable) {
				t = treeMapper.map(entry.f1);
			}
			out.collect(new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), t));
		}
	}

	private static final class MergeTreeKeySelector<T>
			implements KeySelector<Tuple2<Integer, T>, Integer> {
		private int level;

		public MergeTreeKeySelector(int level) {
			this.level = level;
		}

		@Override
		public Integer getKey(Tuple2<Integer, T> input) throws Exception {
			return input.f0 >> (level + 1);
		}
	}

	private static final class MergeTreeProjectionMapper<T>
			implements MapFunction<Tuple2<Integer, T>, T> {
		public MergeTreeProjectionMapper() {
		}

		@Override
		public T map(Tuple2<Integer, T> input) throws Exception {
			return input.f1;
		}
	}

}
