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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.example.utils.Candidate;
import org.apache.flink.graph.streaming.example.utils.Degrees;
import org.apache.flink.graph.streaming.example.utils.SignedVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class DegreeCountMergeTreeExample {

	public static final long TOP_DEGREES = 11;

	public DegreeCountMergeTreeExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// Source: http://grouplens.org/datasets/movielens/
		DataStream<Edge<Long, NullValue>> edges = env
				.readTextFile("movielens_10k_sorted.txt")
				.map(new MapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(String s) throws Exception {
						String[] args = s.split("\t");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						return new Edge<>(src, trg, NullValue.getInstance());
					}
				});

		edges
				.flatMap(new InitDegreeMapper())
				.groupBy(0)
				.map(new DegreeCountMapper())
				.groupBy(new MergeTreeKeySelector(0))
				.map(new DegreeCountMapper())
				.groupBy(new MergeTreeKeySelector(1))
				.map(new DegreeCountMapper())
				.map(new TopDegreeMapper())
				.print();

		JobExecutionResult res = env.execute("Distributed Merge Tree Sandbox");
		long runtime = res.getNetRuntime();
		System.out.println("Runtime: " + runtime);
	}

	private static final class TopDegreeMapper implements MapFunction<Degrees, Degrees> {
		@Override
		public Degrees map(Degrees input) throws Exception {

			ValueComparator bvc =  new ValueComparator(input.getMap());
			TreeMap<Long,Long> sortedDegrees = new TreeMap<>(bvc);
			sortedDegrees.putAll(input.getMap());

			int i = 0;
			Degrees result = new Degrees(0, false);

			for (Map.Entry<Long, Long> entry : sortedDegrees.descendingMap().entrySet()) {
				if (i >= TOP_DEGREES) {
					break;
				}
				result.set(entry.getKey(), entry.getValue());
				i++;
			}
			return result;
		}
	}

	private static final class DegreeCountMapper extends RichMapFunction<Degrees, Degrees> {

		Degrees degrees = null;
		int self;

		public DegreeCountMapper() { }

		@Override
		public Degrees map(Degrees input) throws Exception {
			self = getRuntimeContext().getIndexOfThisSubtask();

			// Propagate first input
			if (degrees == null) {
				degrees = new Degrees(self, input, true);
				return degrees;
			}

			// Merge or sum new degrees
			if (input.getMerge()) {
				// Merge degrees
				degrees.set(input.getMap());
				return degrees;
			} else {
				// Sum degrees
				degrees.add(input.getMap());
				return degrees;
			}
		}
	}

	private static final class InitDegreeMapper implements
			FlatMapFunction<Edge<Long,NullValue>, Degrees> {

		@Override
		public void flatMap(Edge<Long, NullValue> edge, Collector<Degrees> out) throws Exception {
			long src = Math.min(edge.getSource(), edge.getTarget());
			long trg = Math.max(edge.getSource(), edge.getTarget());

			Degrees srcDegree = new Degrees(0, false);
			srcDegree.set(src, 1);

			Degrees trgDegree = new Degrees(0, false);
			trgDegree.set(trg, 1);

			out.collect(srcDegree);
			out.collect(trgDegree);
		}
	}

	private static final class MergeTreeKeySelector implements KeySelector<Degrees, Integer> {
		private int level;

		public MergeTreeKeySelector(int level) {
			this.level = level;
		}

		@Override
		public Integer getKey(Degrees input) throws Exception {
			return input.getSource() >> (level + 1);
		}
	}

	private static final class ValueComparator implements Comparator<Long> {

		Map<Long, Long> base;
		public ValueComparator(Map<Long, Long> base) {
			this.base = base;
		}

		@Override
		public int compare(Long a, Long b) {
			if (base.get(a) >= base.get(b)) {
				return 1;
			}
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		new DegreeCountMergeTreeExample();
	}
}
