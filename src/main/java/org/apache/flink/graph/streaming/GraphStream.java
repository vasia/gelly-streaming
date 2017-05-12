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

import java.io.Serializable;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;

/**
 * The super-class of all graph stream types.
 * 
 * @param <K> the vertex ID type
 * @param <VV> the vertex value type
 * @param <EV> the edge value type
 */
public abstract class GraphStream<K, VV, EV> {

	/**
	 * @return the Flink streaming execution environment.
	 */
	public abstract StreamExecutionEnvironment getContext();

	/**
	 * @return the vertex DataStream.
	 */
	public abstract DataStream<Vertex<K, VV>> getVertices();

	/**
	 * @return the edge DataStream.
	 */
	public abstract DataStream<Edge<K, EV>> getEdges();

	/**
	 * Apply a function to the attribute of each edge in the graph stream.
	 *
	 * @param mapper the map function to apply.
	 * @return a new graph stream.
	 */
	public abstract <NV> GraphStream<K, VV, NV> mapEdges(final MapFunction<Edge<K, EV>, NV> mapper);

	/**
	 * Apply a filter to each vertex in the graph stream
	 * Since this is an edge-only stream, the vertex filter can only access the key of vertices
	 *
	 * @param filter the filter function to apply.
	 * @return the filtered graph stream.
	 */
	public abstract GraphStream<K, VV, EV> filterVertices(FilterFunction<Vertex<K, NullValue>> filter);

	/**
	 * Apply a filter to each edge in the graph stream
	 *
	 * @param filter the filter function to apply.
	 * @return the filtered graph stream.
	 */
	public abstract GraphStream<K, VV, EV> filterEdges(FilterFunction<Edge<K, EV>> filter);

	/**
	 * Removes the duplicate edges by storing a neighborhood set for each vertex
	 *
	 * @return a graph stream with no duplicate edges
	 */
	public abstract GraphStream<K, VV, EV> distinct();

	/**
	 * Get the degree stream
	 *
	 * @return a stream of vertices, with the degree as the vertex value
	 * @throws Exception
	 */
	public abstract DataStream<Vertex<K, Long>> getDegrees() throws Exception;

	/**
	 * Get the in-degree stream
	 *
	 * @return a stream of vertices, with the in-degree as the vertex value
	 * @throws Exception
	 */
	public abstract DataStream<Vertex<K, Long>> getInDegrees() throws Exception;

	/**
	 * Get the out-degree stream
	 *
	 * @return a stream of vertices, with the out-degree as the vertex value
	 * @throws Exception
	 */
	public abstract DataStream<Vertex<K, Long>> getOutDegrees() throws Exception;

	/**
	 * @return a data stream representing the number of all edges in the streamed graph, including possible duplicates
	 */
	public abstract DataStream<Long> numberOfEdges();

	/**
	 * @return a continuously improving data stream representing the number of vertices in the streamed graph
	 */
	public abstract DataStream<Long> numberOfVertices();

	/**
	 * @return a graph stream where edges are undirected
	 */
	public abstract GraphStream<K, VV, EV> undirected();

	/**
	 * @return a graph stream with the edge directions reversed
	 */
	public abstract GraphStream<K, VV, EV> reverse();

	/**
	 * Applies an incremental aggregation on a graphstream and returns a stream of aggregation results 
	 * 
	 * @param summaryAggregation
	 * @param <S>
	 * @param <T>
     * @return
     */
	public abstract <S extends Serializable, T> DataStream<T> aggregate(
			SummaryAggregation<K,EV,S,T> summaryAggregation);
}