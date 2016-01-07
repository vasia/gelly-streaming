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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.util.Candidates;
import org.apache.flink.graph.streaming.example.util.SignedVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The bipartiteness check example tests whether an input graph is bipartite
 * or not. A bipartite graph's vertices can be separated into two disjoint
 * groups, such as no two nodes inside the same group is connected by an edge.
 * The example uses the merge-tree abstraction of our graph streaming API.
 */
public class BipartitenessCheckRemake implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		GraphStream<Long, NullValue> graph = new GraphStream<>(getEdgesDataSet(env), env);
		
		FoldFunction<Edge<Long, NullValue>, Candidates> updFun = new FoldFunction<Edge<Long, NullValue>, Candidates>() {
			@Override
			public Candidates fold(Candidates candidates, Edge<Long, NullValue> edge) throws Exception {
				return candidates.merge(edgeToCandidate(edge));
			}
		};
		
		ReduceFunction<Candidates> combineFun = new ReduceFunction<Candidates>() {
			@Override
			public Candidates reduce(Candidates c1, Candidates c2) throws Exception {
				return c1.merge(c2);
			}
		};
		
		DataStream bipartition = graph.aggregate(new WindowGraphAggregation<>(updFun, combineFun, new Candidates(true), 500, false));
		
		// Emit the results
		if (fileOutput) {
			bipartition.writeAsCsv(outputPath);
		} else {
			bipartition.print();
		}

		env.execute("Bipartiteness Check");
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	public static Candidates edgeToCandidate(Edge<Long, NullValue> edge) throws Exception {
		long src = Math.min(edge.getSource(), edge.getTarget());
		long trg = Math.max(edge.getSource(), edge.getTarget());
		Candidates cand = new Candidates(true);
		cand.add(src, new SignedVertex(src, true));
		cand.add(src, new SignedVertex(trg, false));
		return cand;
	}

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage: BipartitenessCheck <input edges path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing BipartitenessCheck example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: BipartitenessCheck <input edges path> <output path>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataStream<Edge<Long, NullValue>> getEdgesDataSet(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return env.readTextFile(edgeInputPath)
					.map(new MapFunction<String, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(String s) throws Exception {
							String[] fields = s.split("\\t");
							long src = Long.parseLong(fields[0]);
							long trg = Long.parseLong(fields[1]);
							return new Edge<>(src, trg, NullValue.getInstance());
						}
					});
		}

		return env.generateSequence(1, 100).flatMap(
				new FlatMapFunction<Long, Edge<Long, NullValue>>() {
					@Override
					public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) throws Exception {
						for (int i = 0; i < 10; i++) {
							long target = key * 2 + 1;
							out.collect(new Edge<>(key, target, NullValue.getInstance()));
						}
					}
				});
	}

	@Override
	public String getDescription() {
		return "Bipartiteness Check";
	}
}
