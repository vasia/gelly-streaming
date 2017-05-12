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
 * in the {@link SnapshotStream#reduceOnEdges(EdgesReduce)} method.
 *
 * @param <EV> the edge value type
 */
public interface EdgesReduce<EV> extends Function, Serializable {

	/**
	 * Combines two edge values into one value of the same type.
	 * The reduceEdges function is consecutively applied to all pairs of edges of a neighborhood,
	 * until only a single value remains.
	 * 
	 * @param firstEdgeValue the value of the first edge
	 * @param secondEdgeValue the value of the second edge
	 * @return The data stream that is the result of applying the reduceEdges function to the graph window.
	 * @throws Exception
	 */
	EV reduceEdges(EV firstEdgeValue, EV secondEdgeValue) throws Exception;
}
