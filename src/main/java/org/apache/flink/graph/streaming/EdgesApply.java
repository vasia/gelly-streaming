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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Interface to be implemented by the function applied to a vertex neighborhood
 * in the {@link SnapshotStream#applyOnNeighbors(EdgesApply)} method.
 *
 * @param <K> the vertex ID type
 * @param <EV> the edge value type
 * @param <T> the accumulator type
 */
public interface EdgesApply<K, EV, T> extends Function, Serializable {

	/**
	 * Computes a custom function on the neighborhood of a vertex.
	 * The vertex can output zero, one or more result values. 
	 * 
	 * @param vertexID the vertex ID
	 * @param neighbors the neighbors of this vertex. The first field of the tuple contains
	 * the neighbor ID and the second field contains the edge value.
	 * @param out the collector to emit the result
	 * @throws Exception
	 */
	void applyOnEdges(K vertexID, Iterable<Tuple2<K, EV>> neighbors, Collector<T> out) throws Exception;
}
