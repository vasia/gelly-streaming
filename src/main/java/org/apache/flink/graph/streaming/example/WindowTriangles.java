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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.streaming.EdgesApply;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * Counts exact number of triangles in a graph slice.
 */
public class WindowTriangles implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SimpleEdgeStream<Long, NullValue> edges = getGraphStream(env);

        edges.slice(Time.of(windowTime, TimeUnit.MILLISECONDS), EdgeDirection.ALL)
        	.applyOnNeighbors(new GenerateCandidateEdges())
        	.keyBy(0, 1).timeWindow(Time.of(windowTime, TimeUnit.MILLISECONDS))
			.apply(new CountTriangles())
			.timeWindowAll(Time.of(windowTime, TimeUnit.MILLISECONDS)).sum(0)
			.print();

        env.execute("Naive window triangle count");
    }


    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

	@SuppressWarnings("serial")
	public static final class GenerateCandidateEdges implements
			EdgesApply<Long, NullValue, Tuple3<Long, Long, Boolean>> {

		@Override
		public void applyOnEdges(Long vertexID,
				Iterable<Tuple2<Long, NullValue>> neighbors,
				Collector<Tuple3<Long, Long, Boolean>> out) throws Exception {

			Tuple3<Long, Long, Boolean> outT = new Tuple3<>();
			outT.setField(vertexID, 0);
			outT.setField(false, 2); //isCandidate=false

			List<Long> neighborIds = new ArrayList<Long>();
			for (Tuple2<Long, NullValue> t: neighbors) {
				outT.setField(t.f0, 1);
				out.collect(outT);
				neighborIds.add(t.f0);
			}
			outT.setField(true, 2); //isCandidate=true
			for (int i=0; i<neighborIds.size()-1; i++) {
				for (int j=i; j<neighborIds.size(); j++) {
					// only emit the candidates
					// with IDs larger than the vertex ID
					if ((neighborIds.get(i) > vertexID) && (neighborIds.get(j) > vertexID)) {
						outT.setField(neighborIds.get(i), 0);
						outT.setField(neighborIds.get(j), 1);
						out.collect(outT);
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class CountTriangles implements 
			WindowFunction<Tuple3<Long, Long, Boolean>, Tuple1<Integer>, Tuple, TimeWindow>{

		@Override
		public void apply(Tuple key, TimeWindow window,
				Iterable<Tuple3<Long, Long, Boolean>> values,
				Collector<Tuple1<Integer>> out) throws Exception {
			int candidates = 0;
			int edges = 0;
			for (Tuple3<Long, Long, Boolean> t: values) {
				if (t.f2) { // candidate
					candidates++;							
				}
				else {
					edges++;
				}
			}
			if (edges > 0) {
				out.collect(new Tuple1<Integer>(candidates));
			}
		}
	}

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static long windowTime = 1000;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage: WindowTriangles <input edges path> <window time (ms)>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			windowTime = Long.parseLong(args[1]);
		} else {
			System.out.println("Executing WindowTriangles example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: WindowTriangles <input edges path> <window time (ms)>");
		}
		return true;
	}


    @SuppressWarnings("serial")
	private static SimpleEdgeStream<Long, NullValue> getGraphStream(StreamExecutionEnvironment env) {

    	if (fileOutput) {
			return new SimpleEdgeStream<Long, NullValue>(env.readTextFile(edgeInputPath)
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
    	return new SimpleEdgeStream<Long, NullValue>(
    			env.generateSequence(1, 10).flatMap(
    				new FlatMapFunction<Long, Edge<Long, NullValue>>() {
    					@Override
    					public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) throws Exception {
    						for (int i = 1; i < 3; i++) {
    							long target = key + i;
    							out.collect(new Edge<>(key, target, NullValue.getInstance()));
    						}
    					}
    				}), env);
    }

    @SuppressWarnings("serial")
	public static final class FlattenSet implements FlatMapFunction<DisjointSet<Long>, Tuple2<Long, Long>> {

    	private Tuple2<Long, Long> t = new Tuple2<>();

		@Override
		public void flatMap(DisjointSet<Long> set, Collector<Tuple2<Long, Long>> out) {
			for (Long vertex : set.getMatches().keySet()) {
	            Long parent = set.find(vertex);
	            t.setField(vertex, 0);
	            t.setField(parent, 1);
	            out.collect(t);
			}
		}
	}

    @SuppressWarnings("serial")
	public static final class IdentityFold implements FoldFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		public Tuple2<Long, Long> fold(Tuple2<Long, Long> accumulator,
				Tuple2<Long, Long> value) throws Exception {
			return value;
		}
    }

    @Override
    public String getDescription() {
        return "Streaming Connected Components on Global Aggregation";
    }
}
