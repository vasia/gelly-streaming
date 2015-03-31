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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;

/**
 *
 * Represents a streamed graph consisting of {@link org.apache.flink.graph.Edge edges}
 * and {@link org.apache.flink.graph.Vertex vertices}.
 *
 * @see org.apache.flink.graph.Edge
 * @see org.apache.flink.graph.Vertex
 *
 * @param <K> the key type for edge and vertex identifiers.
 * @param <VV> the value type for vertexes.
 * @param <EV> the value type for edges.
 */
public interface GraphStream<K extends Comparable<K> & Serializable, VV extends Serializable, EV extends Serializable> {

	/**
	 * @return the flink streaming execution environment.
	 */
	public StreamExecutionEnvironment getContext();

	/**
	 * @return the vertex DataStream.
	 */
	public DataStream<Vertex<K, VV>> getVertices();

	/**
	 * @return the edge DataStream.
	 */
	public DataStream<Edge<K, EV>> getEdges();

	/**
	 * Apply a function to the attribute of each vertex in the graph stream.
	 *
	 * @param map the map function to apply.
	 * @return a new graph stream.
	 */
	public <NV extends Serializable> GraphStream<K, NV, EV> mapVertices(final MapFunction<Vertex<K, VV>, NV> map);

	/**
	 * Apply a function to the attribute of each edge in the graph stream.
	 *
	 * @param map the map function to apply.
	 * @return a new graph stream.
	 */
	public <NV extends Serializable> GraphStream<K, VV, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> map);

	/**
	 * Apply a filter to each vertex in the graph stream
	 *
	 * @param filter the filter function to apply.
	 * @return a filtered graph stream.
	 */
	public GraphStream<K, VV, EV> filterVertices(final FilterFunction<Vertex<K, VV>> filter);

	/**
	 * Apply a filter to each edge in the graph stream
	 *
	 * @param filter the filter function to apply.
	 * @return a filtered graph stream.
	 */
	public GraphStream<K, VV, EV> filterEdges(final FilterFunction<Edge<K, EV>> filter);

}
