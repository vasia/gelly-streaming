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
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.types.NullValue;

import java.util.PrimitiveIterator;
import java.util.SplittableRandom;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 * <p>
 * This is a single-pass implementation, which uses a {@link WindowGraphAggregation} to periodically merge
 * the partitioned state. For an iterative implementation, see {@link IterativeConnectedComponents}.
 */
public class ConnectedComponentsExample implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		final Integer mergeWindowTime = 2000;
		final int numVertices = 1000000;
		final int numEdges = 10000000;
		final int numTasks = 4;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.getConfig().disableSysoutLogging();
		env.setParallelism(numTasks);

		GraphStream<Integer, NullValue, NullValue> edges = getRandomGraphStream(env, numEdges, numTasks, numVertices);

		DataStream<DisjointSet<Integer>> cc = edges.aggregate(new ConnectedComponents<>(mergeWindowTime));

		cc.addSink(new SinkFunction() {
			public void invoke(Object value) throws Exception {
			}
		});

		JobExecutionResult res = env.execute("Streaming Connected Components");
		System.out.println("time: " + res.getNetRuntime());
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************
	

	private static GraphStream<Integer, NullValue, NullValue> getRandomGraphStream(StreamExecutionEnvironment env,
																				   final int numEdges, final int numTasks, final int numVertices) {

		DataStream<Edge<Integer, NullValue>> edgeStream = env.addSource(new ParallelSourceFunction<Edge<Integer, NullValue>>() {
			public void run(SourceContext<Edge<Integer, NullValue>> ctx) throws Exception {

				final SplittableRandom rand = new SplittableRandom();
				final int totalEdges = numEdges / numTasks;
				PrimitiveIterator.OfInt randStr = rand.ints(totalEdges, 0, numVertices).iterator();
				final Edge<Integer, NullValue> out = new Edge<>();
				out.setField(NullValue.getInstance(), 2);
				while (randStr.hasNext()) {
					out.setField(Integer.valueOf(randStr.nextInt()), 0);
					out.setField(Integer.valueOf(randStr.nextInt()), 1);
					ctx.collect(out);
				}
			}

			@Override
			public void cancel() {
			}

		});
		return new SimpleEdgeStream<>(edgeStream, env);
	}


	@Override
	public String getDescription() {
		return "Streaming Connected Components on Global Aggregation";
	}
}