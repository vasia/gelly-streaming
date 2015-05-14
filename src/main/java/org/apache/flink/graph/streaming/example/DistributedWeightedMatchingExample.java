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

public class DistributedWeightedMatchingExample {

	public static final int PARALLELISM = 4;

	public DistributedWeightedMatchingExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(PARALLELISM);

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

		DataStream<MatchingEvent> events = edges.flatMap(new PartitioningMapper());
		IterativeDataStream<MatchingEvent> iteration = events.groupBy(0).iterate();

		SplitDataStream<MatchingEvent> step = iteration
				.flatMap(new ScatterMatchingMapper())
				.groupBy(0)
				.flatMap(new GatherMatchingMapper())
				.split(new MatchingSplitter());

		iteration.closeWith(step.select("iterate"));

		step.select("output").print(); // .map(new MatchingSumMapper()).groupBy(0).sum(1).print();

		JobExecutionResult res = env.execute("Weighted Matching Example");
		long runtime = res.getNetRuntime();
		System.out.println("Runtime: " + runtime);
	}

	private static final class ScatterMatchingMapper implements FlatMapFunction<MatchingEvent, MatchingEvent> {
		private final Set<Edge<Long, Long>> localMatchings;

		public ScatterMatchingMapper() {
			localMatchings = new HashSet<>();
		}

		@Override
		public void flatMap(MatchingEvent event, Collector<MatchingEvent> out) throws Exception {
			Edge<Long, Long> edge = event.getEdge();

			System.out.println("Scatter: " + event);

			// Edge arriving from the stream
			if (event.getType() == MatchingEvent.Type.ADD) {
				// Forward any possible collisions to master
				List<Edge<Long, Long>> collidingEdges = findCollisions(edge);

				if (collidingEdges.isEmpty()) {
					out.collect(new MatchingEvent(true, MatchingEvent.Type.ADD, edge,
							edge, edge));
				} else if (collidingEdges.size() == 1) {
					out.collect(new MatchingEvent(true, MatchingEvent.Type.ADD, edge,
							collidingEdges.get(0), edge));
				} else if (collidingEdges.size() == 2) {
					out.collect(new MatchingEvent(true, MatchingEvent.Type.ADD, edge,
							collidingEdges.get(0), collidingEdges.get(1)));
				}
			} else if (event.getType() == MatchingEvent.Type.REPLACE) {
				// boolean isMaster = event.getTarget() == event.getMaster();
				localMatchings.remove(event.getCollisionA());
				localMatchings.remove(event.getCollisionB());
				localMatchings.add(edge);
			}
		}

		private List<Edge<Long, Long>> findCollisions(Edge<Long, Long> edge) {
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

	private static final class GatherMatchingMapper implements FlatMapFunction<MatchingEvent, MatchingEvent> {

		private final Map<Edge<Long, Long>, MatchingEvent> receivedCollisions;

		public GatherMatchingMapper() {
			receivedCollisions = new HashMap<>();
		}

		@Override
		public void flatMap(MatchingEvent event, Collector<MatchingEvent> out) throws Exception {
			Edge<Long, Long> edge = event.getEdge();

			System.out.println("Gather: " + event);

			// Save the collisions to the map
			if (!receivedCollisions.containsKey(edge)) {
				receivedCollisions.put(edge, event);
				return;
			}

			MatchingEvent otherEvent = receivedCollisions.remove(edge);
			List<Edge<Long, Long>> collisions = new ArrayList<>();

			if (!event.getCollisionA().equals(edge)) {
				collisions.add(event.getCollisionA());
			}
			if (!event.getCollisionB().equals(edge)) {
				collisions.add(event.getCollisionB());
			}
			if (!otherEvent.getCollisionA().equals(edge)) {
				collisions.add(otherEvent.getCollisionA());
			}
			if (!otherEvent.getCollisionB().equals(edge)) {
				collisions.add(otherEvent.getCollisionB());
			}

			long sum = 0;
			for (Edge<Long, Long> collidingEdge : collisions) {
				sum += collidingEdge.getValue();
			}

			if (edge.getValue() > 2 * sum) {
				Edge<Long, Long> collisionA = (collisions.size() == 0) ? edge : collisions.get(0);
				Edge<Long, Long> collisionB = (collisions.size() == 0) ? edge :
						(collisions.size() == 2) ? collisions.get(1) : edge;

				out.collect(new MatchingEvent(true, MatchingEvent.Type.REPLACE, edge, collisionA, collisionB));
				out.collect(new MatchingEvent(false, MatchingEvent.Type.REPLACE, edge, collisionA, collisionB));

				for (Edge<Long, Long> collidingEdge : collisions) {
					out.collect(new MatchingEvent(MatchingEvent.Type.REMOVE, collidingEdge));
				}

				out.collect(new MatchingEvent(MatchingEvent.Type.ADD, edge));
			}
		}
	}

	private static final class PartitioningMapper implements FlatMapFunction<Edge<Long, Long>, MatchingEvent> {
		@Override
		public void flatMap(Edge<Long, Long> edge, Collector<MatchingEvent> out) throws Exception {
			// Make sure src < trg
			long src = Math.min(edge.getSource(), edge.getTarget());
			long trg = Math.max(edge.getSource(), edge.getTarget());
			Edge<Long, Long> newEdge = new Edge<>(src, trg, edge.getValue());

			// Wrap the edge in a MatchingEvent
			out.collect(new MatchingEvent(true, MatchingEvent.Type.ADD, newEdge, newEdge, newEdge));
			out.collect(new MatchingEvent(false, MatchingEvent.Type.ADD, newEdge, newEdge, newEdge));
		}
	}

	private static final class MatchingSplitter implements OutputSelector<MatchingEvent> {
		@Override
		public Iterable<String> select(MatchingEvent event) {
			List<String> output = new ArrayList<>();
			if (event.getType() == MatchingEvent.Type.REPLACE) {
				output.add("iterate");
			} else {
				output.add("output");
			}
			return output;
		}
	}

	private static final class MatchingSumMapper implements MapFunction<MatchingEvent, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(MatchingEvent event) throws Exception {
			if (event.getType() == MatchingEvent.Type.ADD) {
				return new Tuple2<>(0L, event.getEdge().getValue());
			}
			return new Tuple2<>(0L, -event.getEdge().getValue());
		}
	}

	public static void main(String[] args) throws Exception {
		new DistributedWeightedMatchingExample();
	}
}
