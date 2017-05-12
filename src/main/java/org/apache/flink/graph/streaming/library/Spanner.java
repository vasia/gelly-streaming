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
import org.apache.flink.graph.streaming.summaries.AdjacencyListGraph;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * The Spanner library method continuously computes a k-Spanner of an insertion-only edge stream.
 * The user-defined parameter k defines the distance estimation error,
 * i.e. a k-spanner preserves all distances with a factor of up to k.
 * <p>
 * This is a single-pass implementation, which uses a {@link SummaryBulkAggregation} to periodically merge
 * the partitioned state.
 *
 * @param <K>  the vertex ID type
 * @param <EV> the edge value type
 */
public class Spanner<K extends Comparable<K>, EV> extends SummaryBulkAggregation<K, EV, AdjacencyListGraph<K>, AdjacencyListGraph<K>> implements Serializable {

	private final int k;

	/**
	 * Creates a Spanner instance.
	 *
	 * @param mergeWindowTime Window time in millisec for the merger.
	 * @param k the distance error factor
	 */
	public Spanner(long mergeWindowTime, int k) {
		super(new UpdateLocal(k), new CombineSpanners(k), new AdjacencyListGraph<K>(), mergeWindowTime, false);
		this.k = k;
	}

	/**
	 * Decide to add or remove an edge to the local spanner in the current window.
	 * If the current distance between the edge endpoints is <= k then the edge is dropped,
	 * otherwise it is added to the local spanner.
	 *
	 * @param <K> the vertex ID type
	 */
	public final static class UpdateLocal<K extends Comparable<K>> implements EdgesFold<K, NullValue, AdjacencyListGraph<K>> {

		private final int factorK;

		public UpdateLocal(int k) {
			factorK = k;
		}

		@Override
		public AdjacencyListGraph<K> foldEdges(AdjacencyListGraph<K> g, K src, K trg, NullValue value) throws Exception {
			if (!g.boundedBFS(src, trg, factorK)) {
				// the current distance between src and trg is > k
				g.addEdge(src, trg);
			}
			return g;
		}
	}

	/**
	 * Merge the local spanners of each partition into the global spanner.
	 */
	public static class CombineSpanners<K extends Comparable<K>> implements ReduceFunction<AdjacencyListGraph<K>> {

		private final int factorK;

		public CombineSpanners(int k) {
			factorK = k;
		}

		@Override
		public AdjacencyListGraph<K> reduce(AdjacencyListGraph<K> g1, AdjacencyListGraph<K> g2) throws Exception {
			// merge the smaller spanner into the larger one
			if (g1.getAdjacencyMap().size() > g2.getAdjacencyMap().size()) {
				for (K src : g2.getAdjacencyMap().keySet()) {
					for (K trg : g2.getAdjacencyMap().get(src)) {
						if (!g1.boundedBFS(src, trg, factorK)) {
							// the current distance between src and trg is > k
							g1.addEdge(src, trg);
						}
					}
				}
				return g1;
			}
			else {
				for (K src : g1.getAdjacencyMap().keySet()) {
					for (K trg : g1.getAdjacencyMap().get(src)) {
						if (!g2.boundedBFS(src, trg, factorK)) {
							// the current distance between src and trg is > k
							g2.addEdge(src, trg);
						}
					}
				}
				return g2;
			}
		}
	}
}
