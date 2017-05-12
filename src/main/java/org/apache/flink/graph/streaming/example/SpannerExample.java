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

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.SummaryBulkAggregation;
import org.apache.flink.graph.streaming.library.Spanner;
import org.apache.flink.graph.streaming.summaries.AdjacencyListGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * This example uses the Spanner library method to continuously compute
 * a k-Spanner of an insertion-only edge stream.
 * The user-defined parameter k defines the distance estimation error,
 * i.e. a k-spanner preserves all distances with a factor of up to k.
 * <p>
 * This is a single-pass implementation, which uses a {@link SummaryBulkAggregation} to periodically merge
 * the partitioned state.
 */
public class SpannerExample implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		GraphStream<Long, NullValue, NullValue> edges = getGraphStream(env);

		DataStream<AdjacencyListGraph<Long>> spanner = edges.aggregate(new Spanner<Long, NullValue>(mergeWindowTime, k));

		// flatten the elements of the spanner and
		// in windows of printWindowTime
		spanner.flatMap(new FlattenSet())
				.keyBy(0).timeWindow(Time.of(printWindowTime, TimeUnit.MILLISECONDS))
				.fold(new Tuple2<>(0l, 0l), new IdentityFold()).print();

		env.execute("Streaming Spanner");
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static long mergeWindowTime = 1000;
	private static long printWindowTime = 2000;
	private static int k = 3;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length != 3) {
				System.err.println("Usage: SpannerExample <input edges path> <merge window time (ms)> "
						+ "<print window time (ms)> <distance factor>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			mergeWindowTime = Long.parseLong(args[1]);
			printWindowTime = Long.parseLong(args[2]);
			k = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing Spanner example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: SpannerExample <input edges path> <merge window time (ms)> "
					+ "<print window time (ms)> <distance factor>");
		}
		return true;
	}


	@SuppressWarnings("serial")
	private static GraphStream<Long, NullValue, NullValue> getGraphStream(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
					.map(new MapFunction<String, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(String s) {
							String[] fields = s.split("\\s");
							long src = Long.parseLong(fields[0]);
							long trg = Long.parseLong(fields[1]);
							return new Edge<>(src, trg, NullValue.getInstance());
						}
					}), env);
		}

		return new 	SimpleEdgeStream<>(env.fromElements(
				new Edge<>(1l, 4l, NullValue.getInstance()),
				new Edge<>(4l, 7l, NullValue.getInstance()),
				new Edge<>(7l, 8l, NullValue.getInstance()),
				new Edge<>(4l, 8l, NullValue.getInstance()),
				new Edge<>(4l, 5l, NullValue.getInstance()),
				new Edge<>(5l, 6l, NullValue.getInstance()),
				new Edge<>(2l, 3l, NullValue.getInstance()),
				new Edge<>(3l, 4l, NullValue.getInstance()),
				new Edge<>(3l, 6l, NullValue.getInstance()),
				new Edge<>(8l, 9l, NullValue.getInstance()),
				new Edge<>(6l, 8l, NullValue.getInstance()),
				new Edge<>(5l, 9l, NullValue.getInstance())), env);
	}

	@SuppressWarnings("serial")
	public static final class FlattenSet implements FlatMapFunction<AdjacencyListGraph<Long>, Tuple2<Long, Long>> {

		private Tuple2<Long, Long> t = new Tuple2<>();

		@Override
		public void flatMap(AdjacencyListGraph<Long> g, Collector<Tuple2<Long, Long>> out) {
			for (Long src : g.getAdjacencyMap().keySet()) {
				t.setField(src, 0);
				for (Long trg : g.getAdjacencyMap().get(src)) {
					t.setField(trg, 1);
					out.collect(t);
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class IdentityFold implements FoldFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		public Tuple2<Long, Long> fold(Tuple2<Long, Long> accumulator, Tuple2<Long, Long> value) throws Exception {
			return value;
		}
	}

	@Override
	public String getDescription() {
		return "Streaming Spanner on Global Aggregation";
	}
}
