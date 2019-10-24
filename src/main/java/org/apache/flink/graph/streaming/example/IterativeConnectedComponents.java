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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored. 
 * <p>
 * This implementation uses streaming iterations to asynchronously merge state among partitions.
 * For a single-pass implementation, see {@link ConnectedComponentsExample}.
 */
public class IterativeConnectedComponents implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Long, Long>> edges = getEdgesDataSet(env);

		IterativeStream<Tuple2<Long, Long>> iteration = edges.iterate();
		DataStream<Tuple2<Long, Long>> result = iteration.closeWith(
				iteration.keyBy(0).flatMap(new AssignComponents()));

		// Emit the results
		result.print();

		env.execute("Streaming Connected Components");
	}

	@SuppressWarnings("serial")
	public static class AssignComponents extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private HashMap<Long, HashSet<Long>> components = new HashMap<>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
			final long sourceId = edge.f0;
			final long targetId = edge.f1;
			long sourceComp = -1;
			long trgComp = -1;

			// check if the endpoints belong to existing components
			for (Entry<Long, HashSet<Long>> entry : components.entrySet()) {
				if ((sourceComp == -1) || (trgComp == -1)) {
					if (entry.getValue().contains(sourceId)) {
						sourceComp = entry.getKey();
					}
					if (entry.getValue().contains(targetId)) {
						trgComp = entry.getKey();
					}
				}
			}
			if (sourceComp != -1) {
				// the source belongs to an existing component
				if (trgComp != -1) {
					// merge the components
					merge(sourceComp, trgComp, out);
				}
				else {
					// add the target to the source's component
					// and update the component Id if needed
					addToExistingComponent(sourceComp, targetId, out);
				}
			}
			else {
				// the source doesn't belong to any component
				if (trgComp != -1) {
					// add the source to the target's component
					// and update the component Id if needed
					addToExistingComponent(trgComp, sourceId, out);
				}
				else {
					// neither src nor trg belong to any component
					// create a new component and add them in it
					createNewComponent(sourceId, targetId, out);
				}
			}
		}

		private void createNewComponent(long sourceId, long targetId, Collector<Tuple2<Long, Long>> out) {
			long componentId = Math.min(sourceId, targetId);
			HashSet<Long> vertexSet = new HashSet<>();
			vertexSet.add(sourceId);
			vertexSet.add(targetId);
			components.put(componentId, vertexSet);
			out.collect(new Tuple2<Long, Long>(sourceId, componentId));
			out.collect(new Tuple2<Long, Long>(targetId, componentId));
		}

		private void addToExistingComponent(long componentId, long toAdd, Collector<Tuple2<Long, Long>> out) {
			HashSet<Long> vertices = components.remove(componentId);
			if (componentId >= toAdd) {
				// output and update component ID
				for (long v: vertices) {
					out.collect(new Tuple2<Long, Long>(v, toAdd));
				}
				vertices.add(componentId);
				components.put(toAdd, vertices);
			}
			else {
				vertices.add(toAdd);
				components.put(componentId, vertices);
				out.collect(new Tuple2<Long, Long>(toAdd, componentId));
			}
		}

		private void merge(long sourceComp, long trgComp, Collector<Tuple2<Long, Long>> out) {
			HashSet<Long> srcVertexSet = components.remove(sourceComp);
			HashSet<Long> trgVertexSet = components.remove(trgComp);
			long componentId = Math.min(sourceComp, trgComp);
			if (sourceComp == componentId) {
				// collect the trgVertexSet
				if (trgVertexSet!= null) {
					for (long v: trgVertexSet) {
						out.collect(new Tuple2<Long, Long>(v, componentId));
					}
				}
			}
			else {
				// collect the srcVertexSet
				if (srcVertexSet != null) {
					for (long v: srcVertexSet) {
						out.collect(new Tuple2<Long, Long>(v, componentId));
					}
				}
			}
			if (trgVertexSet!= null) {
				srcVertexSet.addAll(trgVertexSet);
			}
			components.put(componentId, srcVertexSet);
		}
		
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 1) {
				System.err.println("Usage: ConnectedComponentsExample <input edges path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
		} else {
			System.out.println("Executing ConnectedComponentsExample example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: ConnectedComponentsExample <input edges path>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataStream<Tuple2<Long, Long>> getEdgesDataSet(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return env.readTextFile(edgeInputPath)
					.map(new MapFunction<String, Tuple2<Long, Long>>() {
						@Override
						public Tuple2<Long, Long> map(String s) {
							String[] fields = s.split("\\t");
							long src = Long.parseLong(fields[0]);
							long trg = Long.parseLong(fields[1]);
							return new Tuple2<>(src, trg);
						}
					});
		}

		return env.generateSequence(1, 10).flatMap(
				new FlatMapFunction<Long, Tuple2<Long, Long>>() {
					@Override
					public void flatMap(Long key, Collector<Tuple2<Long, Long>> out) throws Exception {
						for (int i = 1; i < 3; i++) {
							long target = key + i;
							out.collect(new Tuple2<>(key, target));
						}
					}
				});
	}

	@Override
	public String getDescription() {
		return "Streaming Connected Components";
	}
}
