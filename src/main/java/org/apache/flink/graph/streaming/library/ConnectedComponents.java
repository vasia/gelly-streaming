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
import org.apache.flink.graph.streaming.example.IterativeConnectedComponents;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * The Connected Components library method assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 * <p>
 * This is a single-pass implementation, which uses a {@link SummaryBulkAggregation} to periodically merge
 * the partitioned state. For an iterative implementation, see {@link IterativeConnectedComponents}.
 *
 * @param <K>  the vertex ID type
 * @param <EV> the edge value type
 */
public class ConnectedComponents<K extends Serializable, EV> extends SummaryBulkAggregation<K, EV, DisjointSet<K>, DisjointSet<K>> implements Serializable {

	/**
	 * Creates a ConnectedComponents object using WindowGraphAggregation class.
	 * To find number of Connected Components the ConnectedComponents object is passed as an argument
	 * to the aggregate function of the {@link org.apache.flink.graph.streaming.GraphStream} class.
	 * Creating the ConnectedComponents object sets the EdgeFold, ReduceFunction, Initial Value,
	 * MergeWindow Time and Transient State for using the Window Graph Aggregation class.
	 *
	 * @param mergeWindowTime Window time in millisec for the merger.
	 */
	public ConnectedComponents(long mergeWindowTime) {
		super(new UpdateCC(), new CombineCC(), new DisjointSet<K>(), mergeWindowTime, false);
	}

	/**
	 * Implements EdgesFold Interface, applies foldEdges function to
	 * a vertex neighborhood
	 * The Edge stream is divided into different windows, the foldEdges function
	 * is applied on each window incrementally and the aggregate state for each window
	 * is updated, in this case it checks the connected components in a window. If
	 * there is an edge between two vertices then they become part of a connected component.
	 *
	 * @param <K> the vertex ID type
	 */
	public final static class UpdateCC<K extends Serializable> implements EdgesFold<K, NullValue, DisjointSet<K>> {

		/**
		 * Implements foldEdges method of EdgesFold interface for combining
		 * two edges values into same type using union method of the DisjointSet class.
		 * In this case it computes the connected components in a partition by
		 * by checking which vertices are connected checking their edges, all the connected
		 * vertices are assigned the same component ID.
		 *
		 * @param ds        the initial value and accumulator
		 * @param vertex    the vertex ID
		 * @param vertex2   the neighbor's ID
		 * @param edgeValue the edge value
		 * @return The data stream that is the result of applying the foldEdges function to the graph window.
		 * @throws Exception
		 */
		@Override
		public DisjointSet<K> foldEdges(DisjointSet<K> ds, K vertex, K vertex2, NullValue edgeValue) throws Exception {
			ds.union(vertex, vertex2);
			return ds;
		}
	}

	/**
	 * Implements the ReduceFunction Interface, applies reduce function to
	 * combine group of elements into a single value.
	 * The aggregated states from different windows are combined together
	 * and reduced to a single result.
	 * In this case the values of the vertices belonging to Connected Components form
	 * each window are merged to find the Connected Components for the whole graph.
	 */
		public static class CombineCC<K extends Serializable> implements ReduceFunction<DisjointSet<K>> {

		/**
		 * Implements reduce method of ReduceFunction interface.
		 * Two values of DisjointSet class are combined into one using merge method
		 * of the DisjointSet class.
		 * In this case the merge method takes Connected Components values that includes
		 * the vertices values along with the Component ID they belong from different
		 * windows and merges them, some Connected Components can be combined if they have
		 * some common vertex value. In the end the total Connected Components of the whole graph
		 * along with the vertices values belonging to those components are returned.
		 *
		 *
		 * @param s1 The first value to combine.
		 * @param s2 The second value to combine.
		 * @return The combined value of both input values.
		 * @throws Exception This method may throw exceptions.
		 */
		@Override
		public DisjointSet<K> reduce(DisjointSet<K> s1, DisjointSet<K> s2) throws Exception {
			int count1 = s1.getMatches().size();
			int count2 = s2.getMatches().size();
			if (count1 <= count2) {
				s2.merge(s1);
				return s2;
			}
			s1.merge(s2);
			return s1;
		}
	}
}







