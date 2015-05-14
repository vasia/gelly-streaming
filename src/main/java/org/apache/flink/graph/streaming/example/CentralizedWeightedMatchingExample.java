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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.example.utils.MatchingEvent;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CentralizedWeightedMatchingExample {

	public CentralizedWeightedMatchingExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// Source: http://grouplens.org/datasets/movielens/
		DataStream<Edge<Long, Long>> edges = env
				.generateSequence(0, 7)
				.map(new MapFunction<Long, Edge<Long, Long>>() {
					@Override
					public Edge<Long, Long> map(Long v) throws Exception {
						return new Edge<>(v, (v + 1) % 8, v + 1);
					}
				});
				/*
				.readTextFile("movielens_10k_sorted.txt")
				.map(new MapFunction<String, Edge<Long, Long>>() {
					@Override
					public Edge<Long, Long> map(String s) throws Exception {
						String[] args = s.split("\t");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						long vote = Long.parseLong(args[2]);
						long timestamp = Long.parseLong(args[3]);

						// Pseudo-random value to have more entropy for the matching
						long value = (vote * timestamp) % 1000;

						return new Edge<>(src, trg, value);
					}
				});
				*/


		edges.flatMap(new CentralizedMatchingMapper())
				.map(new MatchingSumMapper())
				.groupBy(0)
				.sum(1)
				.print();

		JobExecutionResult res = env.execute("Weighted Matching Example");
		long runtime = res.getNetRuntime();
		System.out.println("Runtime: " + runtime);
	}

	private static final class CentralizedMatchingMapper implements FlatMapFunction<Edge<Long, Long>,
			Tuple2<CentralizedMatchingMapper.Type, Edge<Long, Long>>> {

		public enum Type {ADD, REMOVE}

		private final Set<Edge<Long, Long>> localMatchings;

		public CentralizedMatchingMapper() {
			localMatchings = new HashSet<>();
		}

		@Override
		public void flatMap(Edge<Long, Long> edge, Collector<Tuple2<Type, Edge<Long, Long>>> out) throws Exception {

			List<Edge<Long, Long>> collidingEdges = findCollision(edge);

			if (collidingEdges.isEmpty()) {
				localMatchings.add(edge);
				out.collect(new Tuple2<>(Type.ADD, edge));
				return;
			}

			long sum = 0;
			for (Edge<Long, Long> collidingEdge : collidingEdges) {
				sum += collidingEdge.getValue();
			}

			if (edge.getValue() > 2 * sum) {
				for (Edge<Long, Long> collidingEdge : collidingEdges) {
					localMatchings.remove(collidingEdge);
					out.collect(new Tuple2<>(Type.REMOVE, collidingEdge));
				}
				localMatchings.add(edge);
				out.collect(new Tuple2<>(Type.ADD, edge));
			}
		}

		private List<Edge<Long, Long>> findCollision(Edge<Long, Long> edge) {
			List<Edge<Long, Long>> results = new ArrayList<>();
			for (Edge<Long, Long> matchedEdge : localMatchings) {
				if ((long) edge.getSource() == matchedEdge.getSource()
						|| (long) edge.getSource() == matchedEdge.getTarget()
						|| (long) edge.getTarget() == matchedEdge.getSource()
						|| (long) edge.getTarget() == matchedEdge.getTarget()) {
					results.add(matchedEdge);
				}
			}
			return results;
		}
	}

	public static void main(String[] args) throws Exception {
		new CentralizedWeightedMatchingExample();
	}

	private class MatchingSumMapper implements MapFunction<Tuple2<CentralizedMatchingMapper.Type, Edge<Long, Long>>,
			Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Tuple2<CentralizedMatchingMapper.Type, Edge<Long, Long>> value) throws Exception {
			if (value.f0 == CentralizedMatchingMapper.Type.ADD) {
				return new Tuple2<>(0L, value.f1.getValue());
			}
			return new Tuple2<>(0L, -value.f1.getValue());
		}
	}
}
