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

package org.apache.flink.graph.streaming.example;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.util.MatchingEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class CentralizedWeightedMatching {

	public CentralizedWeightedMatching() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// Source: http://grouplens.org/datasets/movielens/
		@SuppressWarnings("serial")
		DataStream<Edge<Long, Long>> edges = env
				.readTextFile("movielens_10k_sorted.txt")
				.map(new MapFunction<String, Edge<Long, Long>>() {
					@Override
					public Edge<Long, Long> map(String s) throws Exception {
						String[] args = s.split("\t");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						long val = Long.parseLong(args[2]) * 10;
						return new Edge<>(src, trg, val);
					}
				});

		GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(edges, env);

		graph.getEdges()
				.flatMap(new WeightedMatchingFlatMapper()).setParallelism(1)
				.print().setParallelism(1);

		JobExecutionResult res = env.execute("Distributed Merge Tree Sandbox");
		long runtime = res.getNetRuntime();
		System.out.println("Runtime: " + runtime);
	}

	@SuppressWarnings("serial")
	private static final class WeightedMatchingFlatMapper 
			implements FlatMapFunction<Edge<Long,Long>, MatchingEvent> {
		private Set<Edge<Long, Long>> localMatching;

		public WeightedMatchingFlatMapper() {
			localMatching = new HashSet<>();
		}

		@Override
		public void flatMap(Edge<Long, Long> edge, Collector<MatchingEvent> out) throws Exception {

			// Find collisions
			Set<Edge<Long, Long>> collisions = new HashSet<>();
			for (Edge<Long, Long> localEdge : localMatching) {
				if (localEdge.getSource().equals(edge.getSource())
						|| localEdge.getSource().equals(edge.getTarget())
						|| localEdge.getTarget().equals(edge.getSource())
						|| localEdge.getTarget().equals(edge.getTarget())) {
					collisions.add(localEdge);
				}
			}

			// Calculate sum
			long sum = 0;
			for (Edge<Long, Long> collidingEdge : collisions) {
				sum += collidingEdge.getValue();
			}

			if (edge.getValue() > 2 * sum) {

				// Remove collisions
				for (Edge<Long, Long> collidingEdge : collisions) {
					localMatching.remove(collidingEdge);
					out.collect(new MatchingEvent(MatchingEvent.Type.REMOVE, collidingEdge));
				}

				localMatching.add(edge);
				out.collect(new MatchingEvent(MatchingEvent.Type.ADD, edge));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		new CentralizedWeightedMatching();
	}
}
