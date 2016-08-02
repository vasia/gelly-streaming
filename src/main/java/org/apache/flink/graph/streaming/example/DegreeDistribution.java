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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.streaming.EventType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * The Degree Distribution algorithm emits a stream of <degree, count>
 * and works for fully dynamic streams of edges, i.e. both edge additions and deletions.
 */
public class DegreeDistribution {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStream<Tuple3<Integer, Integer, EventType>> edges = getGraphStream(env);
		edges.flatMap(
				new FlatMapFunction<Tuple3<Integer,Integer,EventType>, Tuple2<Integer, Integer>>() {
					public void flatMap(Tuple3<Integer, Integer, EventType> t, Collector<Tuple2<Integer, Integer>> c) {
						// output <vertexID, degreeChange>
						int change = t.f2.equals(EventType.EDGE_ADDITION) ? 1 : -1 ;
						c.collect(new Tuple2<>(t.f0, change));
						c.collect(new Tuple2<>(t.f1, change));
					}
				}).keyBy(0).flatMap(
				new FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

					Map<Integer, Integer> verticesWithDegrees = new HashMap<>();

					public void flatMap(Tuple2<Integer, Integer> t, Collector<Tuple2<Integer, Integer>> c) {
						// output <degree, localCount>
						if (verticesWithDegrees.containsKey(t.f0)) {
							// update existing vertex
							int oldDegree = verticesWithDegrees.get(t.f0);
							int newDegree = oldDegree + t.f1;
							if (newDegree > 0) {
								verticesWithDegrees.put(t.f0, newDegree);
								c.collect(new Tuple2<>(newDegree, 1));
							}
							else {
								verticesWithDegrees.remove(t.f0);
							}
							c.collect(new Tuple2<>(oldDegree, -1));
						} else {
							// first time we see this vertex
							verticesWithDegrees.put(t.f0, 1);
							c.collect(new Tuple2<>(1, 1));
						}
					}
				}).keyBy(0).map(
				new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

					Map<Integer, Integer> degreesWithCounts = new HashMap<>();

					public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> t) {
						if (degreesWithCounts.containsKey(t.f0)) {
							// update existing degree
							int newCount = degreesWithCounts.get(t.f0) + t.f1;
							degreesWithCounts.put(t.f0, newCount);
							return new Tuple2<>(t.f0, newCount);
						} else {
							// first time degree
							degreesWithCounts.put(t.f0, t.f1);
							return new Tuple2<>(t.f0, t.f1);
						}
					}
				}).print();

		env.execute("Streaming Degree Distribution");
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 3) {
				System.err.println("Usage: DegreeDistribution <input edges path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
		} else {
			System.out.println("Executing DegreeDistribution example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: DegreeDistribution <input edges path>");
		}
		return true;
	}


	@SuppressWarnings("serial")
	private static DataStream<Tuple3<Integer, Integer, EventType>> getGraphStream(StreamExecutionEnvironment env) {

		if (fileOutput) {
			env.readTextFile(edgeInputPath)
					.map(new MapFunction<String, Tuple3<Integer, Integer, EventType>>() {
						@Override
						public Tuple3<Integer, Integer, EventType> map(String s) {
							String[] fields = s.split("\\s");
							int src = Integer.parseInt(fields[0]);
							int trg = Integer.parseInt(fields[1]);
							EventType t = fields[2].equals("+") ? EventType.EDGE_ADDITION : EventType.EDGE_DELETION;
							return new Tuple3<>(src, trg, t);
						}
					});
		}

		return env.fromElements(
				new Tuple3<>(1, 2, EventType.EDGE_ADDITION),
				new Tuple3<>(2, 3, EventType.EDGE_ADDITION),
				new Tuple3<>(1, 4, EventType.EDGE_ADDITION),
				new Tuple3<>(2, 3, EventType.EDGE_DELETION),
				new Tuple3<>(3, 4, EventType.EDGE_ADDITION));
	}
}
