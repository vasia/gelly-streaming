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

import io.netty.util.internal.ConcurrentSet;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
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
import java.util.Set;

public class BipartiteMergeTreeExample {

	public BipartiteMergeTreeExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// The "square" bipartite graph
		List<String> sampleStream = new ArrayList<>();
		sampleStream.add("1,2");
		sampleStream.add("3,4");
		sampleStream.add("1,3");
		sampleStream.add("2,4");

		// A failing configuration
		// Remove the edge (4,5) to make it bipartite again
		List<String> sampleStream2 = new ArrayList<>();
		sampleStream2.add("1,2");
		sampleStream2.add("2,3");
		sampleStream2.add("1,4");
		// sampleStream2.add("4,5");
		sampleStream2.add("3,5");
		sampleStream2.add("1,5");
		sampleStream2.add("3,6");
		sampleStream2.add("4,7");

		DataStream<Edge<Long, NullValue>> edges = env.fromCollection(sampleStream2)
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

	private static final class MergeTreeMapper extends RichFlatMapFunction<Tuple4<Long,Set<Long>,Set<Long>,Boolean>,
			Tuple4<Long,Set<Long>,Set<Long>,Boolean>> {

		// TODO: figure out what causes a concurrent modification exception and fix it
		private Map<Long, ConcurrentSet<SetPair>> pool;
		private long id;

		public MergeTreeMapper() {
			pool = new HashMap<>();
		}

		@Override
		public void flatMap(Tuple4<Long, Set<Long>, Set<Long>, Boolean> input,
				Collector<Tuple4<Long, Set<Long>, Set<Long>, Boolean>> out) throws Exception {

			id = getRuntimeContext().getIndexOfThisSubtask();
			long inputId = input.f0;
			SetPair inputPair = new SetPair(input.f1, input.f2);

			// If a failure was already found, propagate it
			if (!input.f3) {
				collectPair(out, null);
				return;
			}

			// If the pool is empty, just return the set-pair
			if (pool.isEmpty()) {
				addToPool(inputId, inputPair);
				collectPair(out, inputPair);
				return;
			}

			// Run optimizations on the pool entries from the same mapper
			boolean skip = optimizePool(inputId, inputPair);

			if (skip) {
				return;
			}

			// Combine the new set-pair with the other part of the pool
			combinePool(inputId, inputPair, out);

			// Add the new pair to the pool
			addToPool(inputId, inputPair);
		}

		private void combinePool(long inputId, SetPair pair,
				Collector<Tuple4<Long, Set<Long>, Set<Long>, Boolean>> out) {

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
			if (!pool.containsKey(inputId)) {
				pool.put(inputId, new ConcurrentSet<SetPair>());
			}

			pool.get(inputId).add(pair);
		}

		private void collectPair(Collector<Tuple4<Long, Set<Long>, Set<Long>, Boolean>> out, SetPair pair) {
			SetPair result = pair;
			Boolean success = true;

			if (result == null) {
				Set<Long> empty = new HashSet<>();
				result = new SetPair(empty, empty);

				success = false;
			}

			out.collect(new Tuple4<>(id, result.getPos(), result.getNeg(), success));
		}

	}

	private static final class MergeTreeKeySelector
			implements KeySelector<Tuple4<Long,Set<Long>,Set<Long>,Boolean>, Long> {

		private int level;

		public MergeTreeKeySelector(int level) {
			this.level = level;
		}

		@Override
		public Long getKey(Tuple4<Long, Set<Long>, Set<Long>, Boolean> input) throws Exception {
			return input.f0 >> (level + 1);
		}
	}

	private static final class InitialSetMapper implements
			MapFunction<Edge<Long,NullValue>, Tuple4<Long, Set<Long>, Set<Long>, Boolean>> {
		@Override
		public Tuple4<Long, Set<Long>, Set<Long>, Boolean> map(Edge<Long, NullValue> edge) throws Exception {
			Set<Long> pos = new HashSet<>();
			Set<Long> neg = new HashSet<>();

			pos.add(edge.getSource());
			neg.add(edge.getTarget());

			// -1 indicated that this is the first level of merging
			// in this case, set-pairs have one element each, and need to be merged, even though they have the same id
			// as such, set-pairs with this "special" id can be merged in MergeTreeMapper
			return new Tuple4<>(-1L, pos, neg, true);
		}
	}

	private static final class SetPair extends Tuple2<Set<Long>, Set<Long>> {

		public SetPair(Set<Long> pos, Set<Long> neg) {
			this.f0 = pos;
			this.f1 = neg;
		}

		public Set<Long> getPos() {
			return this.f0;
		}

		public Set<Long> getNeg() {
			return this.f1;
		}

		@Override
		public SetPair copy() {
			Set<Long> pos = new HashSet<>();
			pos.addAll(this.f0);

			Set<Long> neg = new HashSet<>();
			neg.addAll(this.f1);

			return new SetPair(pos, neg);
		}
	}

	public static void main(String[] args) throws Exception {
		new BipartiteMergeTreeExample();
	}
}
