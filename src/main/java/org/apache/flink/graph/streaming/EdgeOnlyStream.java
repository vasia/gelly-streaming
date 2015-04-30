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
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
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
public class EdgeOnlyStream<K extends Comparable<K> & Serializable, EV extends Serializable> {

	private final StreamExecutionEnvironment context;
	private final DataStream<Edge<K, EV>> edges;
	private final GraphConfiguration configuration;

	private final DataStream<Vertex<K, Long>> degrees;
	private final DataStream<Vertex<K, Long>> inDegrees;
	private final DataStream<Vertex<K, Long>> outDegrees;

	/**
	 * Creates a graph from an edge stream
	 *
	 * @param edges a DataStream of edges.
	 * @param context the flink execution environment.
	 */
	public EdgeOnlyStream(DataStream<Edge<K, EV>> edges, StreamExecutionEnvironment context) {
		this.edges = edges;
		this.context = context;
		this.configuration = new GraphConfiguration();

		this.degrees = null;
		this.inDegrees = null;
		this.outDegrees = null;
	}

	/**
	 * Creates a graph from an edge stream, with a custom configuration
	 *
	 * @param edges a DataStream of edges.
	 * @param context the flink execution environment.
	 * @param configuration the custom properties of the streamed graph.
	 */
	public EdgeOnlyStream(DataStream<Edge<K, EV>> edges, StreamExecutionEnvironment context,
			GraphConfiguration configuration) {
		this.edges = edges;
		this.context = context;
		this.configuration = configuration;

		if (this.configuration.getCollectDegrees()) {
			this.degrees = this.edges
					.flatMap(new DegreeTypeSeparator<K, EV>(true, true))
					.groupBy(0)
					.map(new DegreeMapFunction<K>());
		} else {
			this.degrees = null;
		}

		if (this.configuration.getCollectInDegrees()) {
			this.inDegrees = this.edges
					.flatMap(new DegreeTypeSeparator<K, EV>(true, false))
					.groupBy(0)
					.map(new DegreeMapFunction<K>());
		} else {
			this.inDegrees = null;
		}

		if (this.configuration.getCollectOutDegrees()) {
			this.outDegrees = this.edges
					.flatMap(new DegreeTypeSeparator<K, EV>(false, true))
					.groupBy(0)
					.map(new DegreeMapFunction<K>());
		} else {
			this.outDegrees = null;
		}
	}

	private static final class DegreeTypeSeparator <K extends Comparable<K> & Serializable, EV extends Serializable>
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

	private static final class DegreeMapFunction <K extends Comparable<K> & Serializable>
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

	private static final class EmitSrcAndTarget<K extends Comparable<K> & Serializable, EV extends Serializable>
			implements FlatMapFunction<Edge<K, EV>, Vertex<K, NullValue>> {

		@Override
		public void flatMap(Edge<K, EV> edge, Collector<Vertex<K, NullValue>> out) throws Exception {

			out.collect(new Vertex<>(edge.getSource(), NullValue.getInstance()));
			out.collect(new Vertex<>(edge.getTarget(), NullValue.getInstance()));
		}
	}

	private static final class FilterDistinctVertices<K extends Comparable<K> & Serializable>
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
	public <NV extends Serializable> EdgeOnlyStream<K, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper) {
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) edges.getType()).getTypeAt(0);
		DataStream<Edge<K, NV>> mappedEdges = edges.map(new ApplyMapperToEdgeWithType<>(mapper,
				keyType));
		return new EdgeOnlyStream<>(mappedEdges, this.context);
	}

	private static final class ApplyMapperToEdgeWithType<K extends Comparable<K> & Serializable,
			EV extends Serializable, NV extends Serializable> implements MapFunction
			<Edge<K, EV>, Edge<K, NV>>, ResultTypeQueryable<Edge<K, NV>> {

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

	private static final class ApplyVertexFilterToEdges<K extends Comparable<K> & Serializable,
			EV extends Serializable> implements FilterFunction<Edge<K, EV>> {

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

		DataStream<Edge<K, EV>> remainingEdges = this.edges
				.filter(filter);

		return new EdgeOnlyStream<>(remainingEdges, this.context);
	}

	/**
	 * Get the degree stream
	 *
	 * @return a stream of vertices, with the degree as the vertex value
	 * @throws Exception
	 */
	public DataStream<Vertex<K, Long>> getDegrees() throws Exception {
		if (!configuration.getCollectDegrees()) {
			throw new Exception("Degrees are not collected in this graph stream");
		}
		return this.degrees;
	}

	/**
	 * Get the in-degree stream
	 *
	 * @return a stream of vertices, with the in-degree as the vertex value
	 * @throws Exception
	 */
	public DataStream<Vertex<K, Long>> getInDegrees() throws Exception {
		if (!configuration.getCollectInDegrees()) {
			throw new Exception("In-degrees are not collected in this graph stream");
		}
		return this.inDegrees;
	}

	/**
	 * Get the out-degree stream
	 *
	 * @return a stream of vertices, with the out-degree as the vertex value
	 * @throws Exception
	 */
	public DataStream<Vertex<K, Long>> getOutDegrees() throws Exception {
		if (!configuration.getCollectOutDegrees()) {
			throw new Exception("Oout-degrees are not collected in this graph stream");
		}
		return this.outDegrees;
	}

}
