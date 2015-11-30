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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.example.util.Candidate;
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
public class BipartitenessCheck implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		// Process bipartiteness
		GraphStream<Long, NullValue> graph = new GraphStream<>(edges, env);
		DataStream<Candidate> bipartition = graph.mergeTree(new InitCandidateMapper(),
				new BipartitenessMapper(), 1000l);

		// Emit the results
		if (fileOutput) {
			bipartition.writeAsCsv(outputPath);
		} else {
			bipartition.print();
		}

		env.execute("Bipartiteness Check");
	}

	// *************************************************************************
	//     BIPARTITENESS FUNCTIONS
	// *************************************************************************


	private static final class BipartitenessMapper implements MapFunction<Candidate, Candidate> {
		private Candidate candidate = null;
		private boolean failed = false;

		public Candidate map(Candidate input) throws Exception {

			// Propagate failure
			if (!input.getSuccess() || failed) {
				return fail();
			}

			// Store and forward the first candidate
			if (candidate == null) {
				candidate = new Candidate(true, input);
				return candidate;
			}

			// Compare each input component with each candidate component and merge accordingly
			for (Map.Entry<Long, Map<Long, SignedVertex>> inEntry : input.getMap().entrySet()) {

				List<Long> mergeWith = new ArrayList<>();

				for (Map.Entry<Long, Map<Long, SignedVertex>> selfEntry : candidate.getMap().entrySet()) {
					long selfKey = selfEntry.getKey();

					// If the two components are exactly the same, skip them
					if (inEntry.getValue().keySet().containsAll(selfEntry.getValue().keySet())
							&& selfEntry.getValue().keySet().containsAll(inEntry.getValue().keySet())) {
						continue;
					}

					// Find vertices of input component in the candidate component
					for (long inVertex : inEntry.getValue().keySet()) {
						if (selfEntry.getValue().containsKey(inVertex)) {
							if (!mergeWith.contains(selfKey)) {
								mergeWith.add(selfKey);
								break;
							}
						}
					}
				}

				if (mergeWith.isEmpty()) {
					// If the input component is disjoint from all components of the candidate,
					// simply add that component
					candidate.add(inEntry.getKey(), inEntry.getValue());
				} else {
					// Merge the input with the lowest id component in candidate
					Collections.sort(mergeWith);
					long firstKey = mergeWith.get(0);
					boolean success;

					success = merge(input, inEntry.getKey(), firstKey);
					if (!success) {
						return fail();
					}

					firstKey = Math.min(inEntry.getKey(), firstKey);

					// Merge other components of candidate into the lowest id component
					for (int i = 1; i < mergeWith.size(); ++i) {

						success = merge(candidate, mergeWith.get(i), firstKey);
						if (!success) {
							fail();
						}

						candidate.getMap().remove(mergeWith.get(i));
					}
				}
			}

			return candidate;
		}

		private boolean merge(Candidate input, long inputKey, long selfKey) throws Exception {
			Map<Long, SignedVertex> inputComponent = input.getMap().get(inputKey);
			Map<Long, SignedVertex> selfComponent = candidate.getMap().get(selfKey);

			// Find the vertices to merge along
			List<Long> mergeBy = new ArrayList<>();

			for (long inputVertex : inputComponent.keySet()) {
				if (selfComponent.containsKey(inputVertex)) {
					mergeBy.add(inputVertex);
				}
			}

			// Determine if the merge should be with reversed signs or not
			boolean inputSign = inputComponent.get(mergeBy.get(0)).getSign();
			boolean selfSign = selfComponent.get(mergeBy.get(0)).getSign();
			boolean reversed = inputSign != selfSign;

			// Evaluate the merge
			boolean success = true;
			for (long mergeVertex : mergeBy) {
				inputSign = inputComponent.get(mergeVertex).getSign();
				selfSign = selfComponent.get(mergeVertex).getSign();
				if (reversed) {
					success = inputSign != selfSign;
				} else {
					success = inputSign == selfSign;
				}
				if (!success) {
					return false;
				}
			}

			// Execute the merge
			long commonKey = Math.min(inputKey, selfKey);

			// Merge input vertices
			for (SignedVertex inputVertex : inputComponent.values()) {

				if (reversed) {
					success = candidate.add(commonKey, inputVertex.reverse());
				} else {
					success = candidate.add(commonKey, inputVertex);
				}
				if (!success) {
					return false;
				}
			}

			return true;
		}

		private Candidate fail() {
			failed = true;
			return new Candidate(false);
		}
	}

	private static final class InitCandidateMapper implements
			FlatMapFunction<Edge<Long,NullValue>, Candidate> {

		@Override
		public void flatMap(Edge<Long, NullValue> edge, Collector<Candidate> out) throws Exception {
			long src = Math.min(edge.getSource(), edge.getTarget());
			long trg = Math.max(edge.getSource(), edge.getTarget());

			Candidate candidate = new Candidate(true);
			candidate.add(src, new SignedVertex(src, true));
			candidate.add(src, new SignedVertex(trg, false));

			out.collect(candidate);
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

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
