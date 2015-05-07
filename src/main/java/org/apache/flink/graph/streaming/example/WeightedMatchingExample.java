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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WeightedMatchingExample {

	public WeightedMatchingExample() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		// Source: http://grouplens.org/datasets/movielens/
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

		edges.print();

		JobExecutionResult res = env.execute("Weighted Matching Example");
		long runtime = res.getNetRuntime();
		System.out.println("Runtime: " + runtime);
	}

	public static void main(String[] args) throws Exception {
		new WeightedMatchingExample();
	}
}
