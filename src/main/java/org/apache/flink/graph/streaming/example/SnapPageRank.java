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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.*;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;


/**
 * A version of PageRank implemented on snapshot streams
 */
public class SnapPageRank implements ProgramDescription{


	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		SimpleEdgeStream<Long, NullValue> edges = getGraphStream(env);
		SnapshotStream<Long, NullValue> snapshotStream= edges.slice(Time.milliseconds(sliceWindow));
		((SingleOutputStreamOperator<Tuple2<Long, Double>>) snapshotStream.run(new PageRankIteration(), 10))
				.returns(new TypeHint<Tuple2<Long, Double>>() {});

		env.execute("Streaming Page Rank");
		
	}                                                                                                                      	
	
	private static class PageRankIteration extends GraphSnapshotIteration<Long, Double, Double, Tuple2<Long, Double>> implements ResultTypeQueryable<Tuple2<Long, Double>>{
		
		@Override
		public Double initialState() {
			return 1.0d;
		}

		@Override
		public void preCompute(VertexContext<Long, Double, Double, Tuple2<Long, Double>> vertexCtx) {
			for(long neighbor: vertexCtx.getNeighbors()){
				vertexCtx.sendMessage(neighbor, vertexCtx.getVertexState());
			}
		}

		@Override
		public void compute(VertexContext<Long, Double, Double, Tuple2<Long, Double>> vertexCtx, Iterable<GraphMessage<Long, Double>> inputMessages) {
			double sum = 0;
			
			for(GraphMessage<Long, Double> msg: inputMessages){
				sum += msg.getValue();
			}
			
			long numNeighbors = Iterables.size(vertexCtx.getNeighbors()); 
			vertexCtx.setVertexState(0.15 / (double) numNeighbors + 0.85 * sum);
			double dividedRank = vertexCtx.getVertexState() / (double) numNeighbors;

			for(long neighbor: vertexCtx.getNeighbors()){
				vertexCtx.sendMessage(neighbor, dividedRank);
			}
		}

		@Override
		public void postCompute(VertexContext<Long, Double, Double, Tuple2<Long, Double>> vertexCtx, Collector<Tuple2<Long, Double>> out) {
			out.collect(new Tuple2<>(vertexCtx.getVertexID(), vertexCtx.getVertexState()));
		}

		@Override
		public TypeInformation<Double> getMessageType() {
			return TypeInformation.of(Double.class);
		}

		@Override
		public TypeInformation<Tuple2<Long, Double>> getProducedType() {
			return new TupleTypeInfo<>(TypeInformation.of(Long.class), TypeInformation.of(Double.class));
		}
	}
	
	@Override
	public String getDescription() {
		return "PageRank on Snapshot Streams";
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static long sliceWindow = 2000;
	

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 1) {
				System.err.println("Usage: ConnectedComponentsExample <input edges path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
		} else {
			System.out.println("Executing Page Rank example with default parameters and built-in default data.");
		}
		return true;
	}


	@SuppressWarnings("serial")
	private static SimpleEdgeStream<Long, NullValue> getGraphStream(StreamExecutionEnvironment env) {

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

		return new SimpleEdgeStream<>(env.generateSequence(1, 100).flatMap(
				new FlatMapFunction<Long, Edge<Long, Long>>() {
					@Override
					public void flatMap(Long key, Collector<Edge<Long, Long>> out) throws Exception {
						out.collect(new Edge<>(key, key + 2, key * 100));
					}
				}),
				new AscendingTimestampExtractor<Edge<Long, Long>>() {
					@Override

					public long extractAscendingTimestamp(Edge<Long, Long> element) {
						return element.getValue();
					}
				}, env).mapEdges(new MapFunction<Edge<Long, Long>, NullValue>() {
			@Override
			public NullValue map(Edge<Long, Long> edge) {
				return NullValue.getInstance();
			}
		});
	}
	
}
