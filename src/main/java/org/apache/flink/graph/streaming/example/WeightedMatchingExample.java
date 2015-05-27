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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.example.utils.MatchingEvent;
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

public class WeightedMatchingExample {

	public static final int PARALLELISM = 4;

	public WeightedMatchingExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(PARALLELISM);

		/*
		DataStream<Edge<Long, Long>> edges = env
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
		DataStream<Edge<Long, Long>> edges = env
				.generateSequence(0, 7)
				.map(new MapFunction<Long, Edge<Long, Long>>() {
					@Override
					public Edge<Long, Long> map(Long v) throws Exception {
						return new Edge<>(v, (v + 1) % 8, v + 1);
					}
				});

		DataStream<MatchingEvent> events = edges.flatMap(new InitialMatchingEventMapper());

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

	private static final class ScatterMatchingMapper extends RichFlatMapFunction<MatchingEvent, MatchingEvent> {
		private final Set<Edge<Long, Long>> matchings;
		private final List<MatchingEvent> queue;
		private Edge<Long, Long> waitFor;

		public ScatterMatchingMapper() {
			this.matchings = new HashSet<>();
			this.queue = new ArrayList<>();
			this.waitFor = null;
		}

		@Override
		public void flatMap(MatchingEvent event, Collector<MatchingEvent> out) throws Exception {
			schedule(event, out);
		}

		public void schedule(MatchingEvent event, Collector<MatchingEvent> out) throws Exception {
			switch (event.getType()) {
				case INIT:
					if (waitFor != null) {
						queue.add(event);
					} else {
						waitFor = event.getEdge();
						process(event, out);
					}
					break;
				case UNLOCK:
					if (waitFor != null && waitFor.equals(event.getEdge())) {
						waitFor = null;

						if (!queue.isEmpty()) {
							schedule(queue.remove(0), out);
						}
					}
					break;
				default:
					if (waitFor != null && !waitFor.equals(event.getEdge())) {
						queue.add(event);
					} else {
						process(event, out);
						waitFor = null;

						if (!queue.isEmpty()) {
							schedule(queue.remove(0), out);
						}
					}
					break;
			}
		}

		public void process(MatchingEvent event, Collector<MatchingEvent> out) throws Exception {
			// System.out.println("Scatter: " + event);
			// System.out.println("Queue: " + queue);

			switch (event.getType()) {
				case INIT:
					// Send all collisions to the master
					Set<Edge<Long, Long>> collisions = findCollisions(event.getEdge());
					out.collect(new MatchingEvent(hash(event.getEdge(), true), MatchingEvent.Type.INIT,
							event.getEdge(), collisions));
					break;
				case REPLACE:
					long id = getRuntimeContext().getIndexOfThisSubtask();

					// Remove all colliding edges
					for (Edge<Long, Long> collidingEdge : event.getCollisions()) {
						matchings.remove(collidingEdge);

						// Send final remove event (only from master)
						if (id == hash(collidingEdge, true)) {
							out.collect(new MatchingEvent(0, MatchingEvent.Type.REMOVE, collidingEdge));
						}
					}

					// Add the new edge
					if (id == hash(event.getEdge(), true) || id == hash(event.getEdge(), false)) {
						matchings.add(event.getEdge());
					}

					// Send final add event (only from master)
					if (id == hash(event.getEdge(), true)) {
						out.collect(new MatchingEvent(0, MatchingEvent.Type.ADD, event.getEdge()));
					}

					break;
			}
		}

		private Set<Edge<Long, Long>> findCollisions(Edge<Long, Long> edge) {
			Set<Edge<Long, Long>> collisions = new HashSet<>();
			for (Edge<Long, Long> matchedEdge : matchings) {
				if (matchedEdge.getSource().equals(edge.getSource())
						|| matchedEdge.getSource().equals(edge.getTarget())
						|| matchedEdge.getTarget().equals(edge.getSource())
						|| matchedEdge.getTarget().equals(edge.getTarget())) {
					collisions.add(matchedEdge);
				}
			}
			return collisions;
		}
	}

	private static final class GatherMatchingMapper implements FlatMapFunction<MatchingEvent, MatchingEvent> {
		private final Map<Edge<Long, Long>, MatchingEvent> partialEvents;

		public GatherMatchingMapper() {
			this.partialEvents = new HashMap<>();
		}

		@Override
		public void flatMap(MatchingEvent event, Collector<MatchingEvent> out) throws Exception {
			// System.out.println("Gather: " + event);

			switch (event.getType()) {
				case INIT:
					// Store all events and only process them once both master and slave sent their collisions
					if (!partialEvents.containsKey(event.getEdge())) {
						partialEvents.put(event.getEdge(), event);
						return;
					}

					MatchingEvent otherEvent = partialEvents.get(event.getEdge());

					// Process the collisions
					Set<Edge<Long, Long>> allCollisions = otherEvent.getCollisions();
					allCollisions.addAll(event.getCollisions());
					long weightSum = 0;

					for (Edge<Long, Long> collidingEdge : allCollisions) {
						weightSum += collidingEdge.getValue();
					}

					if (event.getEdge().getValue() > weightSum * 2) {
						// Replace the collisions with the new edge
						Set<Long> targets = new HashSet<>();
						targets.add(hash(event.getEdge(), true));
						targets.add(hash(event.getEdge(), false));

						for (Edge<Long, Long> collidingEdge : allCollisions) {
							targets.add(hash(collidingEdge, true));
							targets.add(hash(collidingEdge, false));
						}

						// Send replace events for the scattering mapper
						for (Long target : targets) {
							out.collect(new MatchingEvent(target, MatchingEvent.Type.REPLACE,
									event.getEdge(), allCollisions));
						}
					} else {
						// Unlock the edge in the scatter mappers
						out.collect(new MatchingEvent(hash(event.getEdge(), true), MatchingEvent.Type.UNLOCK,
								event.getEdge()));
						out.collect(new MatchingEvent(hash(event.getEdge(), false), MatchingEvent.Type.UNLOCK,
								event.getEdge()));
					}

					break;

				case ADD:
				case REMOVE:
					out.collect(event);
			}
		}
	}

	private static final class MatchingSplitter implements org.apache.flink.streaming.api.collector.selector.OutputSelector<MatchingEvent> {
		@Override
		public Iterable<String> select(MatchingEvent event) {
			List<String> result = new ArrayList<>();

			switch (event.getType()) {
				case REPLACE:
				case UNLOCK:
					result.add("iterate");
					break;
				default:
					result.add("output");
					break;
			}

			return result;
		}
	}

	private static final class InitialMatchingEventMapper implements FlatMapFunction<Edge<Long, Long>, MatchingEvent> {
		@Override
		public void flatMap(Edge<Long, Long> edge, Collector<MatchingEvent> out) throws Exception {
			out.collect(new MatchingEvent(hash(edge, true), MatchingEvent.Type.INIT, edge));
			out.collect(new MatchingEvent(hash(edge, false), MatchingEvent.Type.INIT, edge));
		}
	}

	public static long hash(Edge<Long, Long> edge, boolean toMaster) {
		long h1 = edge.getSource().hashCode() % PARALLELISM;
		long h2 = (h1 + 1) % PARALLELISM; // Debug version

		return toMaster ? h1 : h2;
	}

	public static void main(String[] args) throws Exception {
		new WeightedMatchingExample();
	}
}
