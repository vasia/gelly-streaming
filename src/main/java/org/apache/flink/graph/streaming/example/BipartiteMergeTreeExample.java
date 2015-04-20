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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.example.utils.Candidate;
import org.apache.flink.graph.streaming.example.utils.SetPair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class BipartiteMergeTreeExample {

	public BipartiteMergeTreeExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// Generate a pseudo-random stream of 2048 edges
		Random rnd = new Random(0xDEADBEEF);

		List<Integer> vertices = new ArrayList<>();
		for (int i = 0; i < 10; ++i) {
			vertices.add(i * 2 + 1);
		}

		List<String> edgeList = new ArrayList<>();
		while (!vertices.isEmpty()) {

			int id = rnd.nextInt(vertices.size());
			int vertex = vertices.remove(id);

			edgeList.add(String.format("%d,%d", vertex, vertex + 1));
			edgeList.add(String.format("%d,%d", vertex, vertex + 3));
		}

		// Add this edge to make it fail
		// edgeList.add("5,17");

		DataStream<Edge<Long, NullValue>> edges = env.fromCollection(edgeList)
				.map(new MapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(String s) throws Exception {
						String[] args = s.split(",");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]);
						return new Edge<>(src, trg, NullValue.getInstance());
					}
				});

		edges
				.map(new InitialSetMapper())
				.flatMap(new MergeTreeMapper())
				.groupBy(new MergeTreeKeySelector(0))
				.flatMap(new MergeTreeMapper())
				.groupBy(new MergeTreeKeySelector(1))
				.flatMap(new MergeTreeMapper())
				.print();

		env.execute("Distributed Merge Tree Sandbox");
	}

	private static final class MergeTreeMapper extends RichFlatMapFunction<Candidate, Candidate> {

		private enum Matching {
			Failed, Disjoint, Normal, Reversed
		}

		private CandidatePool pool;
		private long self;

		public MergeTreeMapper() {
			pool = new CandidatePool();
		}

		@Override
		public void flatMap(Candidate input, Collector<Candidate> out) throws Exception {

			self = getRuntimeContext().getIndexOfThisSubtask();

			long id = input.f0;
			Set<SetPair> pairs = input.f1;
			boolean failure = !input.f2;

			// System.out.println("@" + self + ", from: " + id + " -> " + pairs + ", pool: " + pool);

			// If a failure was already found, propagate it
			if (failure) {
				fail(out);
				return;
			}

			// If the pool is empty, just return the set-pair and add the new pair to the pool
			if (pool.isEmpty()) {
				pool.addAll(id, pairs);
				pool.update();

				out.collect(new Candidate(self, pairs, true));
				return;
			}

			Set<SetPair> diff;

			if (!pool.containsKey(id)) {
				pool.addAll(id, pairs);
				pool.update();

				diff = pairs;

			} else {
				// Combine the new set-pair with the same part of the pool
				diff = combine(id, pairs);
			}

			// Check for failure
			if (diff == null) {
				fail(out);
				return;
			}

			// Find the other part of the pool
			long otherId;
			try {
				otherId = pool.otherKey(id);
			} catch (CandidatePool.KeyNotFoundException e) {
				// If it doesn't exist, collect the result of combine
				out.collect(new Candidate(self, diff, true));
				return;
			}

			// Combine the new set-pair with the other part of the pool
			diff = combine(otherId, diff);

			if (diff == null) {
				fail(out);
				return;
			}

			out.collect(new Candidate(self, diff, true));
		}

		private Set<SetPair> combine(long key, Set<SetPair> newSet) {

			Set<SetPair> diff = new HashSet<>();
			boolean allFailed = true;

			for (SetPair newPair : newSet) {
				Set<Long> pos = newPair.getPos();
				Set<Long> neg = newPair.getNeg();

				for (SetPair oldPair : pool.get(key)) {

					// System.out.println("Comparing " + oldPair + " vs " + newPair);
					Matching result = evaluate(oldPair, newPair);
					// System.out.println("Result: " + result.name());

					switch (result) {
						case Failed:
							pool.remove(key, oldPair);
							break;
						case Disjoint:
							allFailed = false;

							// Create all possibilities
							SetPair pairOne = oldPair.copy();
							SetPair pairTwo = oldPair.copy();

							pairOne.getPos().addAll(pos);
							pairOne.getNeg().addAll(neg);

							pairTwo.getPos().addAll(neg);
							pairTwo.getNeg().addAll(pos);

							// Update the pool
							pool.remove(key, oldPair);
							pool.add(key, pairOne);
							pool.add(key, pairTwo);

							diff.add(pairOne);
							diff.add(pairTwo);
							break;

						case Normal:
						case Reversed:
							allFailed = false;

							SetPair pair = oldPair.copy();
							if (result.equals(Matching.Normal)) {
								// Resolve pos -> pos, neg -> neg
								pair.getPos().addAll(pos);
								pair.getNeg().addAll(neg);
							} else {
								// Resolve pos -> neg, neg -> pos
								pair.getPos().addAll(neg);
								pair.getNeg().addAll(pos);
							}

							pool.remove(key, oldPair);
							pool.add(key, pair);
							diff.add(pair);
							break;
					}

				}
			}

			if (allFailed) {
				return null;
			}

			pool.update();
			return diff;
		}

		private Matching evaluate(SetPair oldPair, SetPair newPair) {

			Set<Long> pos = newPair.getPos();
			Set<Long> neg = newPair.getNeg();

			boolean posInPos = false;
			boolean posInNeg = false;
			boolean negInPos = false;
			boolean negInNeg = false;

			for (Long vertex : pos) {
				posInPos |= oldPair.getPos().contains(vertex);
				posInNeg |= oldPair.getNeg().contains(vertex);
			}
			for (Long vertex : neg) {
				negInPos |= oldPair.getPos().contains(vertex);
				negInNeg |= oldPair.getNeg().contains(vertex);
			}

			// Check for failures
			boolean failure = (posInPos && posInNeg) || (negInPos && negInNeg)
					|| (posInPos && negInPos) || (posInNeg && negInNeg);
			if (failure) {
				return Matching.Failed;
			}

			// Check for disjoint sets
			boolean disjoint = !posInPos && !posInNeg && !negInPos && !negInNeg;
			if (disjoint) {
				return Matching.Disjoint;
			}

			if (posInPos || negInNeg) {
				return Matching.Normal;
			}

			return Matching.Reversed;
		}

		private void fail(Collector<Candidate> out) {
			Set<SetPair> empty = new HashSet<>();
			out.collect(new Candidate(self, empty, false));
		}
	}

	private static final class CandidatePool extends HashMap<Long, Set<SetPair>> {

		private Map<Long, Set<SetPair>> toAdd;
		private Map<Long, Set<SetPair>> toRemove;

		public CandidatePool() {
			toAdd = new HashMap<>();
			toRemove = new HashMap<>();
		}

		public void addAll(long key, Set<SetPair> values) {
			for (SetPair pair : values) {
				this.add(key, pair);
			}
		}

		public void add(long key, SetPair value) {
			if (!toAdd.containsKey(key)) {
				toAdd.put(key, new HashSet<SetPair>());
			}
			toAdd.get(key).add(value);
		}

		public void remove(long key, SetPair value) {
			if (!toRemove.containsKey(key)) {
				toRemove.put(key, new HashSet<SetPair>());
			}
			toRemove.get(key).add(value);
		}

		public void update() {

			// Execute removals
			for (Map.Entry<Long, Set<SetPair>> e : toRemove.entrySet()) {
				long key = e.getKey();
				if (!this.containsKey(key)) {
					continue;
				}

				for (SetPair pair : e.getValue()) {
					this.get(key).remove(pair);
				}
			}
			// Execute additions
			for (Map.Entry<Long, Set<SetPair>> e : toAdd.entrySet()) {
				long key = e.getKey();
				if (!this.containsKey(key)) {
					this.put(key, new HashSet<SetPair>());
				}

				for (SetPair pair : e.getValue()) {
					this.get(key).add(pair);
				}
			}

			toAdd.clear();
			toRemove.clear();
		}

		public long otherKey(long key) throws Exception {
			for (long otherKey : this.keySet()) {
				if (otherKey != key) {
					return otherKey;
				}
			}
			throw new KeyNotFoundException("No other key");
		}

		private static final class KeyNotFoundException extends Exception {
			public KeyNotFoundException(String msg) {
				super(msg);
			}
		}
	}

	private static final class MergeTreeKeySelector implements KeySelector<Candidate, Long> {

		private int level;

		public MergeTreeKeySelector(int level) {
			this.level = level;
		}

		@Override
		public Long getKey(Candidate input) throws Exception {
			return input.f0 >> (level + 1);
		}
	}

	private static final class InitialSetMapper implements
			MapFunction<Edge<Long,NullValue>, Candidate> {
		@Override
		public Candidate map(Edge<Long, NullValue> edge) throws Exception {
			Set<Long> pos = new HashSet<>();
			Set<Long> neg = new HashSet<>();

			pos.add(edge.getSource());
			neg.add(edge.getTarget());

			SetPair pair = new SetPair(pos, neg);
			Set<SetPair> set = new HashSet<>();
			set.add(pair);

			return new Candidate(0L, set, true);
		}
	}

	public static void main(String[] args) throws Exception {
		new BipartiteMergeTreeExample();
	}
}
