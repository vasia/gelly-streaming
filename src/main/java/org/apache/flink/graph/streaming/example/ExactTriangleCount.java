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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Single-pass, insertion-only exact Triangle Local and Global Count algorithm.
 * <p>
 * Based on http://www.kdd.org/kdd2016/papers/files/rfp0465-de-stefaniA.pdf.
 */
public class ExactTriangleCount {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);

		DataStream<Tuple2<Integer, Integer>> result =
				edges.buildNeighborhood(false)
				.map(new ProjectCanonicalEdges())
				.keyBy(0, 1).flatMap(new IntersectNeighborhoods())
				.keyBy(0).flatMap(new SumAndEmitCounters());

		if (resultPath != null) {
			result.writeAsText(resultPath);
		}
		else {
			result.print();
		}

		env.execute("Exact Triangle Count");
	}

	// *** Transformation Methods *** //

	/**
	 * Receives 2 tuples from the same edge (src + target) and intersects the attached neighborhoods.
	 * For each common neighbor, increase local and global counters.
	 */
	public static final class IntersectNeighborhoods implements
			FlatMapFunction<Tuple3<Integer, Integer, TreeSet<Integer>>, Tuple2<Integer, Integer>> {

		Map<Tuple2<Integer, Integer>, TreeSet<Integer>> neighborhoods = new HashMap<>();

		public void flatMap(Tuple3<Integer, Integer, TreeSet<Integer>> t, Collector<Tuple2<Integer, Integer>> out) {
			//intersect neighborhoods and emit local and global counters
			Tuple2<Integer, Integer> key = new Tuple2<>(t.f0, t.f1);
			if (neighborhoods.containsKey(key)) {
				// this is the 2nd neighborhood => intersect
				TreeSet<Integer> t1 = neighborhoods.remove(key);
				TreeSet<Integer> t2 = t.f2;
				int counter = 0;
				if (t1.size() < t2.size()) {
					// iterate t1 and search t2
					for (int i : t1) {
						if (t2.contains(i)) {
							counter++;
							out.collect(new Tuple2<>(i, 1));
						}
					}
				} else {
					// iterate t2 and search t1
					for (int i : t2) {
						if (t1.contains(i)) {
							counter++;
							out.collect(new Tuple2<>(i, 1));
						}
					}
				}
				if (counter > 0) {
					//emit counter for srcID, trgID, and total
					out.collect(new Tuple2<>(t.f0, counter));
					out.collect(new Tuple2<>(t.f1, counter));
					// -1 signals the total counter
					out.collect(new Tuple2<>(-1, counter));
				}
			} else {
				// first neighborhood for this edge: store and wait for next
				neighborhoods.put(key, t.f2);
			}
		}
	}

	/**
	 * Sums up and emits local and global counters.
	 */
	public static final class SumAndEmitCounters implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		Map<Integer, Integer> counts = new HashMap<>();

		public void flatMap(Tuple2<Integer, Integer> t, Collector<Tuple2<Integer, Integer>> out) {
			if (counts.containsKey(t.f0)) {
				int newCount = counts.get(t.f0) + t.f1;
				counts.put(t.f0, newCount);
				out.collect(new Tuple2<>(t.f0, newCount));
			} else {
				counts.put(t.f0, t.f1);
				out.collect(new Tuple2<>(t.f0, t.f1));
			}
		}
	}

	public static final class ProjectCanonicalEdges implements
			MapFunction<Tuple3<Integer, Integer, TreeSet<Integer>>, Tuple3<Integer, Integer, TreeSet<Integer>>> {
		@Override
		public Tuple3<Integer, Integer, TreeSet<Integer>> map(Tuple3<Integer, Integer, TreeSet<Integer>> t) {
			int source = Math.min(t.f0, t.f1);
			int trg = Math.max(t.f0, t.f1);
			t.setField(source, 0);
			t.setField(trg, 1);
			return t;
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String resultPath = null;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 2) {
				System.err.println("Usage: ExactTriangleCount <input edges path> <result path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			resultPath = args[1];
		} else {
			System.out.println("Executing ExactTriangleCount example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: ExactTriangleCount <input edges path> <result path>");
		}
		return true;
	}


	@SuppressWarnings("serial")
	private static SimpleEdgeStream<Integer, NullValue> getGraphStream(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
					.flatMap(new FlatMapFunction<String, Edge<Integer, NullValue>>() {
						@Override
						public void flatMap(String s, Collector<Edge<Integer, NullValue>> out) {
							String[] fields = s.split("\\s");
							if (!fields[0].equals("%")) {
								int src = Integer.parseInt(fields[0]);
								int trg = Integer.parseInt(fields[1]);
								out.collect(new Edge<>(src, trg, NullValue.getInstance()));
							}
						}
					}), env);
		}

		return new SimpleEdgeStream<>(env.fromElements(
				new Edge<>(1, 2, NullValue.getInstance()),
				new Edge<>(2, 3, NullValue.getInstance()),
				new Edge<>(2, 6, NullValue.getInstance()),
				new Edge<>(5, 6, NullValue.getInstance()),
				new Edge<>(1, 4, NullValue.getInstance()),
				new Edge<>(5, 3, NullValue.getInstance()),
				new Edge<>(3, 4, NullValue.getInstance()),
				new Edge<>(3, 6, NullValue.getInstance()),
				new Edge<>(1, 3, NullValue.getInstance())), env);
	}
}
