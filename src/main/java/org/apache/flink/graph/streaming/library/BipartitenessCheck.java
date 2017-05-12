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

package org.apache.flink.graph.streaming.library;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.SummaryBulkAggregation;
import org.apache.flink.graph.streaming.summaries.Candidates;
import org.apache.flink.graph.streaming.util.SignedVertex;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * The Bipartiteness check library method checks whether an input graph is bipartite
 * or not. A bipartite graph's vertices can be separated into two disjoint
 * groups, such as no two nodes inside the same group are connected by an edge.
 * The library uses the Window Graph Aggregation class of our graph streaming API.
 *
 * @param <K>  the vertex ID type
 * @param <EV> the edge value type
 */
public class BipartitenessCheck<K extends Serializable, EV> extends SummaryBulkAggregation<K, EV, Candidates, Candidates> implements Serializable {

	/**
	 * Creates a BipartitenessCheck object using WindowGraphAggregation class.
	 * To perform the Bipartiteness check the BipartitenessCheck object is passed as an argument
	 * to the aggregate function of the {@link org.apache.flink.graph.streaming.GraphStream} class.
	 * Creating the Bipartiteness object sets the EdgeFold, ReduceFunction, Initial Value,
	 * MergeWindow Time and Transient State for using the Window Graph Aggregation class.
	 *
	 * @param mergeWindowTime Window time in millisec for the merger.
	 */
	public BipartitenessCheck(long mergeWindowTime) {
		super(new updateFunction(), new combineFunction(), new Candidates(true), mergeWindowTime, false);
	}

	public static Candidates edgeToCandidate(long v1, long v2) throws Exception {
		long src = Math.min(v1, v2);
		long trg = Math.max(v1, v2);
		Candidates cand = new Candidates(true);
		cand.add(src, new SignedVertex(src, true));
		cand.add(src, new SignedVertex(trg, false));
		return cand;
	}

	@SuppressWarnings("serial")

	/**
	 * Implements the EdgesFold Interface, applies foldEdges function to
	 * a vertex neighborhood.
	 * The Edge stream is divided into different windows, the foldEdges function
	 * is applied on each window incrementally and the aggregate state for each window
	 * is updated, in this case it checks the sub-graph(stream of edges) in a window is Bipartite or not.
	 *
	 * @param <K> the vertex ID type
	 */
	public static class updateFunction<K extends Serializable> implements EdgesFold<Long, NullValue, Candidates> {

		/**
		 * Implements foldEdges method of EdgesFold interface for combining
		 * two edges values into same type using merge method of the Candidates class.
		 * In this case it checks the Bipartiteness of the sub-graph in a partition by
		 * separating vertices into two groups such that there is no edge between the
		 * vertices of the same group.
		 * In case the sub-graph is Bipartite it assigns true value to the candidate object's field,
		 * otherwise false.
		 *
		 * @param candidates the initial value and accumulator
		 * @param v1         the vertex ID
		 * @param v2         the neighbor's ID
		 * @param edgeVal    the edge value
		 * @return The data stream that is the result of applying the foldEdges function to the graph window.
		 * @throws Exception
		 */
		@Override
		public Candidates foldEdges(Candidates candidates, Long v1, Long v2, NullValue edgeVal) throws Exception {
			return candidates.merge(edgeToCandidate(v1, v2));
		}
	}

	/**
	 * Implements the ReduceFunction Interface, applies reduce function to
	 * combine group of elements into a single value.
	 * The aggregated states from different windows are combined together
	 * and reduced to a single result.
	 * In this case the Bipartiteness state of sub-graphs in each window is checked
	 * and their aggregate states are merged to check if the whole graph is Bipartite or
	 * not.
	 *
	 */
	public static class combineFunction implements ReduceFunction<Candidates> {

		/**
		 * Implements reduce method of ReduceFunction interface.
		 * Two values of Candidates class are combined into one using merge method
		 * of the Candidate class.
		 * In this case the merge method checks Bipartiteness state i.e true or false
		 * from all windows and merges the aggregate results together to check
		 * if all of vertices can be divided into two groups so that there in no edge between
		 * two vertices of the same group.
		 * In-case two such groups exist it assigns true value to the candidate object's field
		 * declaring the graph as Bipartite.
		 *
		 * @param c1 The first value to combine.
		 * @param c2 The second value to combine.
		 * @return The combined value of both input values.
		 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
		 *                   to fail and may trigger recovery.
		 */
		@Override
		public Candidates reduce(Candidates c1, Candidates c2) throws Exception {
			return c1.merge(c2);
		}
	}

}
