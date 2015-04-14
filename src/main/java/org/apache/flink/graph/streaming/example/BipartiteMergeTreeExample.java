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

import io.netty.util.internal.ConcurrentSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
		for (int i = 0; i < 8; ++i) {
			vertices.add(i * 2 + 1);
		}

		List<String> edgeList = new ArrayList<>();
		while (!vertices.isEmpty()) {

			int id = rnd.nextInt(vertices.size());
			int vertex = vertices.remove(id);

			edgeList.add(String.format("%d,%d", vertex, vertex + 1));
			edgeList.add(String.format("%d,%d", vertex, vertex + 3));
		}

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

	private static final class MergeTreeMapper extends RichFlatMapFunction<Tuple3<Long, SetPair, Boolean>,
			Tuple3<Long, SetPair, Boolean>> {

		// TODO: figure out what causes a concurrent modification exception and fix it
		private Map<Long, ConcurrentSet<SetPair>> pool;

		private Map<Long, ConcurrentSet<SetPair>> addedPairs;
		private Map<Long, ConcurrentSet<SetPair>> removedPairs;

		private long id;

		public MergeTreeMapper() {
			pool = new HashMap<>();

			addedPairs = new HashMap<>();
			removedPairs = new HashMap<>();
		}

		@Override
		public void flatMap(Tuple3<Long, SetPair, Boolean> input,
				Collector<Tuple3<Long, SetPair, Boolean>> out) throws Exception {

			id = getRuntimeContext().getIndexOfThisSubtask();

			long inputId = input.f0;
			SetPair inputPair = input.f1;

			// If a failure was already found, propagate it
			if (!input.f2) {
				collectPair(out, null);
				return;
			}

			// If the pool is empty, just return the set-pair
			if (pool.isEmpty()) {
				collectPair(out, inputPair);

			} else {

				// Run optimizations on the pool entries from the same mapper
				boolean skip = optimizePool(inputId, inputPair);

				if (skip) {
					return;
				}

				// Combine the new set-pair with the other part of the pool
				combinePool(inputId, inputPair, out);
			}

			// Add the new pair to the pool
			addToPool(inputId, inputPair);

			// Execute the additions and removals to/from the pool
			updatePool();

		}

		private void combinePool(long inputId, SetPair pair,
				Collector<Tuple3<Long, SetPair, Boolean>> out) {

			long otherKey = inputId;

			// On the first level, ids are -1, which can be combined with itself
			if (inputId != -1) {
				Iterator<Long> keys = pool.keySet().iterator();

				while (keys.hasNext() && otherKey == inputId) {
					otherKey = keys.next();
				}

				if (!keys.hasNext() && otherKey == inputId) {
					// There is no other key in the pool at the moment
					return;
				}
			}

			// Try to match the new input with all set-pairs in the pool
			ConcurrentSet<SetPair> otherPool = pool.get(otherKey);

			Set<Long> a = pair.getPos();
			Set<Long> b = pair.getNeg();
			boolean allFailed = true;

			for (SetPair otherPair : otherPool) {

				boolean aInPos = false;
				boolean aInNeg = false;
				boolean bInPos = false;
				boolean bInNeg = false;

				for (Long vertex : a) {
					if (otherPair.getPos().contains(vertex)) {
						aInPos = true;
					}
					if (otherPair.getNeg().contains(vertex)) {
						aInNeg = true;
					}
				}
				for (Long vertex : b) {
					if (otherPair.getPos().contains(vertex)) {
						bInPos = true;
					}
					if (otherPair.getNeg().contains(vertex)) {
						bInNeg = true;
					}
				}

				// Failure
				boolean failure = (aInPos && aInNeg) || (bInPos && bInNeg)
						|| (aInPos && bInPos) || (aInNeg && bInNeg);
				if (failure) {
					continue;
				}

				allFailed = false;

				// Disjoint sets
				boolean disjoint = !(aInPos || aInNeg || bInPos || bInNeg);
				if (disjoint) {
					// Save all possibilities to the pool
					SetPair oldPair = otherPair.copy();
					SetPair newPair = otherPair.copy();

					oldPair.getPos().addAll(a);
					oldPair.getNeg().addAll(b);

					newPair.getPos().addAll(b);
					newPair.getNeg().addAll(a);

					// Update the pool
					// No need to add newPair to the pool later on
					removeFromPool(inputId, pair);
					removeFromPool(inputId, otherPair);

					addToPool(inputId, oldPair);
					addToPool(inputId, newPair);

					// Emit them as well
					collectPair(out, oldPair);
					collectPair(out, newPair);
					continue;
				}

				// Resolvable sets
				SetPair newPair = otherPair.copy();

				if (aInPos || bInNeg) {
					newPair.getPos().addAll(a);
					newPair.getNeg().addAll(b);
				} else {
					newPair.getPos().addAll(b);
					newPair.getNeg().addAll(a);
				}

				collectPair(out, newPair);
			}

			if (allFailed) {
				collectPair(out, null);
			}
		}

		private boolean optimizePool(long inputId, SetPair pair) {

			if (!pool.containsKey(inputId)) {
				pool.put(inputId, new ConcurrentSet<SetPair>());
			}

			// Run optimizations on the pool entries from the same mapper
			ConcurrentSet<SetPair> selfPool = pool.get(inputId);

			// Check if the new set-pair is a superset of an old entry
			// In this case, old data can be removed from the pool
			for (SetPair otherPair : selfPool) {
				boolean posInPos = pair.getPos().containsAll(otherPair.getPos())
						&& pair.getNeg().containsAll(otherPair.getNeg());
				boolean negInNeg = pair.getPos().containsAll(otherPair.getNeg())
						&& pair.getNeg().containsAll(otherPair.getPos());

				if (posInPos || negInNeg) {
					selfPool.remove(otherPair);
				}
			}

			// Check if the old data contains a superset of the new
			// In this case, the new data should not be processed at all
			boolean skip = false;

			for (SetPair otherPair : selfPool) {
				boolean PosInPos = otherPair.getPos().containsAll(pair.getPos())
						&& otherPair.getNeg().containsAll(pair.getNeg());
				boolean NegInNeg = otherPair.getNeg().containsAll(pair.getPos())
						&& otherPair.getPos().containsAll(pair.getNeg());

				if (PosInPos || NegInNeg) {
					skip = true;
					break;
				}
			}

			return skip;
		}

		private void addToPool(long inputId, SetPair pair) {
			if (!addedPairs.containsKey(inputId)) {
				addedPairs.put(inputId, new ConcurrentSet<SetPair>());
			}

			addedPairs.get(inputId).add(pair);
		}

		private void removeFromPool(long inputId, SetPair pair) {
			if (!removedPairs.containsKey(inputId)) {
				removedPairs.put(inputId, new ConcurrentSet<SetPair>());
			}

			removedPairs.get(inputId).add(pair);
		}

		private void updatePool() {

			// Execute additions
			for (Map.Entry<Long, ConcurrentSet<SetPair>> e : addedPairs.entrySet()) {
				if (!pool.containsKey(e.getKey())) {
					pool.put(e.getKey(), new ConcurrentSet<SetPair>());
				}

				for (SetPair pair : e.getValue()) {
					pool.get(e.getKey()).add(pair);
				}
			}

			// Execute removals
			for (Map.Entry<Long, ConcurrentSet<SetPair>> e : removedPairs.entrySet()) {
				if (!pool.containsKey(e.getKey())) {
					continue;
				}

				for (SetPair pair : e.getValue()) {
					pool.get(e.getKey()).remove(pair);
				}
			}
		}

		private void collectPair(Collector<Tuple3<Long, SetPair, Boolean>> out, SetPair pair) {
			SetPair result = pair;
			Boolean success = true;

			if (result == null) {
				Set<Long> empty = new HashSet<>();
				result = new SetPair(empty, empty);

				success = false;
			}

			out.collect(new Tuple3<>(id, result, success));
		}

	}

	private static final class MergeTreeKeySelector
			implements KeySelector<Tuple3<Long, SetPair, Boolean>, Long> {

		private int level;

		public MergeTreeKeySelector(int level) {
			this.level = level;
		}

		@Override
		public Long getKey(Tuple3<Long, SetPair, Boolean> input) throws Exception {
			return input.f0 >> (level + 1);
		}
	}

	private static final class InitialSetMapper implements
			MapFunction<Edge<Long,NullValue>, Tuple3<Long, SetPair, Boolean>> {
		@Override
		public Tuple3<Long, SetPair, Boolean> map(Edge<Long, NullValue> edge) throws Exception {
			Set<Long> pos = new HashSet<>();
			Set<Long> neg = new HashSet<>();

			pos.add(edge.getSource());
			neg.add(edge.getTarget());

			SetPair pair = new SetPair(pos, neg);

			// -1 indicates that this is the first level of merging
			// in this case, set-pairs have one element each, and need to be merged, even though they have the same id
			// as such, set-pairs with this "special" id can be merged in MergeTreeMapper
			return new Tuple3<>(-1L, pair, true);
		}
	}

	public static void main(String[] args) throws Exception {
		new BipartiteMergeTreeExample();
	}
}
