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

import org.apache.flink.api.common.functions.Function;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link SnapshotStream#foldNeighbors(Object, EdgesFold)} method.
 *
 * @param <K> the vertex ID type
 * @param <EV> the edge value type
 * @param <T> the accumulator type
 */
public interface EdgesFold<K, EV, T> extends Function, Serializable {

	/**
	 * Combines two edge values into one value of the same type.
	 * The foldEdges function is consecutively applied to all edges of a neighborhood,
	 * until only a single value remains.
	 * 
	 * @param accum the initial value and accumulator
	 * @param vertexID the vertex ID
	 * @param neighborID the neighbor's ID
	 * @param edgeValue the edge value
	 * @return The data stream that is the result of applying the foldEdges function to the graph window.
	 * @throws Exception
	 */
	T foldEdges(T accum, K vertexID, K neighborID, EV edgeValue) throws Exception;
}
