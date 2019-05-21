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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.streaming.EdgesApply;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		if (!parseParameters(args, env)) {
			return;
		}

        SimpleEdgeStream<Long, NullValue> edges = getGraphStream(env);

        DataStream<Tuple2<Integer, Long>> triangleCount = 
        	edges.slice(windowTime, EdgeDirection.ALL)
        	.applyOnNeighbors(new GenerateCandidateEdges())
        	.keyBy(0, 1).timeWindow(windowTime)
			.apply(new CountTriangles())
			.timeWindowAll(windowTime).sum(0);

        if (fileOutput) {
        	triangleCount.writeAsText(outputPath);
        }
        else {
        	triangleCount.print();
        }
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

			Set<Long> neighborIdsSet = new HashSet<Long>();
			for (Tuple2<Long, NullValue> t: neighbors) {
				outT.setField(t.f0, 1);
				out.collect(outT);
				neighborIdsSet.add(t.f0);
			}
			Object[] neighborIds = neighborIdsSet.toArray();
			neighborIdsSet.clear();
			outT.setField(true, 2); //isCandidate=true
			for (int i=0; i<neighborIds.length-1; i++) {
				for (int j=i; j<neighborIds.length; j++) {
					// only emit the candidates
					// with IDs larger than the vertex ID
					if (((long)neighborIds[i] > vertexID) && ((long)neighborIds[j] > vertexID)) {
						outT.setField((long)neighborIds[i], 0);
						outT.setField((long)neighborIds[j], 1);
						out.collect(outT);
					}
				}
			}
		}
	}

	@SuppressWarnings("serial")
	public static final class CountTriangles implements 
			WindowFunction<Tuple3<Long, Long, Boolean>, Tuple2<Integer, Long>, Tuple, TimeWindow>{

		@Override
		public void apply(Tuple key, TimeWindow window,
				Iterable<Tuple3<Long, Long, Boolean>> values,
				Collector<Tuple2<Integer, Long>> out) throws Exception {
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
				out.collect(new Tuple2<Integer, Long>(candidates, window.maxTimestamp()));
			}
		}
	}

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Time windowTime = Time.of(300, TimeUnit.MILLISECONDS);

	private static boolean parseParameters(String[] args, StreamExecutionEnvironment env) {

		if(args.length > 0) {
			if(args.length < 3) {
				System.err.println("Usage: WindowTriangles <input edges path> <output path>"
						+ " <window time (ms)> <parallelism (optional)>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			windowTime = Time.of(Long.parseLong(args[2]), TimeUnit.MILLISECONDS);
			if (args.length > 3) {
				env.setParallelism(Integer.parseInt(args[3]));
			}

		} else {
			System.out.println("Executing WindowTriangles example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: WindowTriangles <input edges path> <output path>"
					+ " <window time (ms)> <parallelism (optional)>");
		}
		return true;
	}


    @SuppressWarnings("serial")
	private static SimpleEdgeStream<Long, NullValue> getGraphStream(StreamExecutionEnvironment env) {

    	if (fileOutput) {
			return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
				.map(new MapFunction<String, Edge<Long, Long>>() {
					@Override
					public Edge<Long, Long> map(String s) {
						String[] fields = s.split("\\s");
						long src = Long.parseLong(fields[0]);
						long trg = Long.parseLong(fields[1]);
						long timestamp = Long.parseLong(fields[2]);
						return new Edge<>(src, trg, timestamp);
					}
				}), new EdgeValueTimestampExtractor(), env).mapEdges(new RemoveEdgeValue());
		}

    	return new SimpleEdgeStream<>(env.generateSequence(1, 10).flatMap(
                new FlatMapFunction<Long, Edge<Long, Long>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, Long>> out) throws Exception {
                    	for (int i = 1; i < 3; i++) {
							long target = key + i;
							out.collect(new Edge<>(key, target, key*100 + (i-1)*50));
						}
                    }
                }), new EdgeValueTimestampExtractor(), env).mapEdges(new RemoveEdgeValue()); 
    }


    @SuppressWarnings("serial")
	public static final class EdgeValueTimestampExtractor extends AscendingTimestampExtractor<Edge<Long, Long>> {
		@Override
		public long extractAscendingTimestamp(Edge<Long, Long> element) {
			return element.getValue();
		}
	}

    @SuppressWarnings("serial")
	public static final class RemoveEdgeValue implements MapFunction<Edge<Long,Long>, NullValue> {
		@Override
		public NullValue map(Edge<Long, Long> edge) {
			return NullValue.getInstance();
		}
	}

    @Override
    public String getDescription() {
        return "Streaming Connected Components on Global Aggregation";
    }
}
