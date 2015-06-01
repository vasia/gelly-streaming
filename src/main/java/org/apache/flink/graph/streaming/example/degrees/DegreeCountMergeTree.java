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

package org.apache.flink.graph.streaming.example.degrees;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgeOnlyStream;
import org.apache.flink.graph.streaming.example.degrees.util.Degrees;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class DegreeCountMergeTree {

	public DegreeCountMergeTree() throws Exception {
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


		EdgeOnlyStream<Long, NullValue> graph = new EdgeOnlyStream<>(edges, env);
		graph.mergeTree(new InitDegreeMapper(), new DegreeCountMapper(), 10000);
				//.print();

		JobExecutionResult res = env.execute("Distributed Merge Tree Sandbox");
		long runtime = res.getNetRuntime();
		System.out.println("Runtime: " + runtime);
	}

	private static final class DegreeCountMapper implements MapFunction<Degrees, Degrees> {

		Degrees degrees = null;
		int self;

		public DegreeCountMapper() { }

		@Override
		public Degrees map(Degrees input) throws Exception {

			// Propagate first input
			if (degrees == null) {
				degrees = new Degrees(input, true);
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

			Degrees srcDegree = new Degrees(false);
			srcDegree.set(src, 1);

			Degrees trgDegree = new Degrees(false);
			trgDegree.set(trg, 1);

			out.collect(srcDegree);
			out.collect(trgDegree);
		}
	}

	public static void main(String[] args) throws Exception {
		new DegreeCountMergeTree();
	}
}
