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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.*;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import java.util.Collection;


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
		SimpleEdgeStream<Integer, NullValue> edges = getGraphStream(env);
		SnapshotStream<Integer, NullValue> snapshotStream = edges.slice(Time.milliseconds(sliceWindow));
		
		//test
//		snapshotStream.applyOnNeighbors(new EdgesApply<Integer, NullValue, String>() {
//					@Override
//					public void applyOnEdges(Integer vertexID, Iterable<Tuple2<Integer, NullValue>> neighbors, Collector<String> out) throws Exception {
//						StringBuffer strbuf = new StringBuffer();
//						strbuf.append(vertexID+":");
//						for(Tuple2<Integer, NullValue> neig : neighbors){
//							strbuf.append(neig.f0+"-");
//						}
//						out.collect(strbuf.toString());
//					}
//				}).keyBy(new KeySelector<String, Integer>() {
//			@Override
//			public Integer getKey(String s) throws Exception {
//				return Integer.valueOf(s.charAt(0));
//			}
//		}).timeWindow(Time.milliseconds(sliceWindow)).apply(new WindowFunction<String, String, Integer, TimeWindow>() {
//			@Override
//			public void apply(Integer integer, TimeWindow timeWindow, Iterable<String> iterable, Collector<String> collector) throws Exception {
//				for(String it : iterable){
//					collector.collect(timeWindow.getTimeContext()+"("+timeWindow.getStart()+", "+timeWindow.getEnd()+") - "+ it);
//				}
//			}
//		}).print();
		
		
		((SingleOutputStreamOperator<Tuple2<Integer, Double>>) snapshotStream.run(new PageRankIteration(), numIterations))
				.returns(new TypeHint<Tuple2<Integer, Double>>() {})
				.print();
		
		
		System.err.println(env.getExecutionPlan());
		env.execute("Streaming Page Rank");
		
	}                                                                                                                      	
	
	private static class PageRankIteration extends GraphSnapshotIteration<Integer, Double, Double, Tuple2<Integer, Double>> implements ResultTypeQueryable<Tuple2<Integer, Double>>{
		
		@Override
		public Double initialState() {
			System.err.println("DEBUG - INIT PHASE");
			return 1.0d;
		}
		
		@Override
		public void preCompute(VertexContext<Integer, Double, Double, Tuple2<Integer, Double>> vertexCtx) {
			System.err.println("DEBUG - PRECOMPUTE PHASE");
			for(int neighbor: vertexCtx.getNeighbors()){
				vertexCtx.sendMessage(neighbor, vertexCtx.getVertexState());
			}
		}

		@Override
		public void compute(VertexContext<Integer, Double, Double, Tuple2<Integer, Double>> vertexCtx, Iterable<GraphMessage<Integer, Double>> inputMessages) {
			System.err.println("DEBUG - COMPUTE PHASE");
			double sum = 0;

			for(GraphMessage<Integer, Double> msg: inputMessages){
				sum += msg.getValue();
			}

			long numNeighbors = Iterables.size(vertexCtx.getNeighbors());
			vertexCtx.setVertexState(0.15 / (double) numNeighbors + 0.85 * sum);
			double dividedRank = vertexCtx.getVertexState() / (double) numNeighbors;

			for(int neighbor: vertexCtx.getNeighbors()){
				vertexCtx.sendMessage(neighbor, dividedRank);
			}
		}

		@Override
		public void postCompute(VertexContext<Integer, Double, Double, Tuple2<Integer, Double>> vertexCtx, Collector<Tuple2<Integer, Double>> out) {
			System.err.println("DEBUG - POSTCOMPUTE PHASE");
			out.collect(new Tuple2<>(vertexCtx.getVertexID(), vertexCtx.getVertexState()));
		}

		@Override
		public TypeInformation<Double> getMessageType() {
			return TypeInformation.of(Double.class);
		}

		@Override
		public TypeInformation<Tuple2<Integer, Double>> getProducedType() {
			return new TupleTypeInfo<>(TypeInformation.of(Integer.class), TypeInformation.of(Double.class));
		}
	}
	
	@Override
	public String getDescription() {
		return "PageRank on Snapshot Streams";
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean parametrized = false;
	private static String dataLocation = null;
	private static long sliceWindow = 1000;
	private static int numIterations = 10;
	
	private static final Collection<Tuple3<Integer, Integer, Long>> sampleStream = Lists.newArrayList(
			new Tuple3<>(1, 2, 1000l),
			new Tuple3<>(1, 3, 1000l),
			new Tuple3<>(1, 4, 1000l),
			new Tuple3<>(1, 2, 2000l),
			new Tuple3<>(1, 3, 2000l),
			new Tuple3<>(1, 4, 2000l),
			new Tuple3<>(1, 2, 3000l),
			new Tuple3<>(1, 3, 3000l),
			new Tuple3<>(1, 4, 4000l),
			new Tuple3<>(1, 2, 5000l)
//			new Tuple3<>(5, 6, 3l),
//			new Tuple3<>(5, 7, 3l),
//			new Tuple3<>(5, 8, 3l),
//			new Tuple3<>(6, 7, 3l),
//			new Tuple3<>(6, 8, 3l),
//			new Tuple3<>(7, 8, 3l)
//			new Tuple3<>(8, 9, 10l)
	);
	

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			parametrized = true;
			dataLocation = args[0];
			sliceWindow = Long.valueOf(args[1]);
			numIterations = Integer.valueOf(args[2]);
		}
		return true;
	}

	private static class PageRankSampleSrc extends RichSourceFunction<Edge<Integer, NullValue>> {
		
		@Override
		public void run(SourceContext<Edge<Integer, NullValue>> ctx) throws Exception {
			long curTime = -1;
			for (Tuple3<Integer, Integer, Long> next:  sampleStream) {
				ctx.collectWithTimestamp(new Edge<>(next.f0, next.f1, NullValue.getInstance()), next.f2);

				if(curTime == -1){
					curTime = next.f2;
				}
				if(curTime < next.f2){
					curTime = next.f2;
					ctx.emitWatermark(new Watermark(curTime-1));
				}
			}
		}

		@Override
		public void cancel() {}
	}



	@SuppressWarnings("serial")
	private static SimpleEdgeStream<Integer, NullValue> getGraphStream(StreamExecutionEnvironment env) {

		if (parametrized) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
			
			return new SimpleEdgeStream<>(env.readTextFile(dataLocation)
					.map(new MapFunction<String, Edge<Integer, NullValue>>() {
						@Override
						public Edge<Integer, NullValue> map(String s) {
							String[] fields = s.split("\\s+");
							int src = Integer.parseInt(fields[0]);
							int trg = Integer.parseInt(fields[1]);
							return new Edge<>(src, trg, NullValue.getInstance());
						}
					}), env);
		}
		else { 
			//TEST MODE
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.setParallelism(4);
			return new SimpleEdgeStream<Integer, NullValue>(env.addSource(new PageRankSampleSrc()), env);
		}
	}
	
}
