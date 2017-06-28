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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphMessage;
import org.apache.flink.graph.streaming.GraphSnapshotIteration;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.VertexContext;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.Collection;


/**
 * A version of SSSP implemented on snapshot streams
 */
public class SnapSSSP implements ProgramDescription{


	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		getGraphStream(env)
				.slice(Time.milliseconds(sliceWindow))
//				.iterateFor(new SSSPIteration(), numIterations)
				.iterateFixpoint(new SSSPIteration())
				.print();
		
		env.execute("Streaming SSSP");
	}                                                                                                                      	
	
	private static class SSSPIteration extends GraphSnapshotIteration<Integer, Double, Double, Tuple2<Integer, Double>> implements ResultTypeQueryable<Tuple2<Integer, Double>>{
		
		@Override
		public Double initialState() {
			return Double.POSITIVE_INFINITY;
		}
		
		@Override
		public void preCompute(VertexContext<Integer, Double, Double, Tuple2<Integer, Double>> vertexCtx) {
//			System.err.println("DEBUG - PRECOMPUTE PHASE");
			for(int neighbor: vertexCtx.getNeighbors()) {
				// if source => send 0
				if (vertexCtx.getVertexID().equals(1)) {
					// set own value to 0
					vertexCtx.setVertexState(0d);
					vertexCtx.sendMessage(neighbor, 0d);
				}
				// if not the source => send nothing
			}
		}

		@Override
		public void compute(VertexContext<Integer, Double, Double, Tuple2<Integer, Double>> vertexCtx, Iterable<GraphMessage<Integer, Double>> inputMessages) {
//			System.err.println("DEBUG - COMPUTE PHASE");
			double distance = vertexCtx.getVertexState();

			for(GraphMessage<Integer, Double> msg: inputMessages){
				// choose the minimum distance
				distance = Math.min(distance, msg.f1 + 1);
			}

			// update state and propagate to neighbors
			if(distance != vertexCtx.getVertexState()) {
				vertexCtx.setVertexState(distance);

				for(int neighbor: vertexCtx.getNeighbors()){
					vertexCtx.sendMessage(neighbor, distance);
				}
			}
		}

		@Override
		public void postCompute(VertexContext<Integer, Double, Double, Tuple2<Integer, Double>> vertexCtx, Collector<Tuple2<Integer, Double>> out) {
//			System.err.println("DEBUG - POSTCOMPUTE PHASE");
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
		return "SSSP on Snapshot Streams";
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean parametrized = false;
	private static String dataLocation = null;
	private static long sliceWindow = 1000;
	private static int numIterations = 5;
	
	private static final Collection<Tuple3<Integer, Integer, Long>> sampleStream = Lists.newArrayList(
			new Tuple3<>(1, 2, 1000l),
			new Tuple3<>(2, 3, 1000l),
			new Tuple3<>(4, 3, 1000l),

			new Tuple3<>(1, 5, 2000l),
			new Tuple3<>(1, 6, 2000l),
			new Tuple3<>(1, 7, 2000l),

			new Tuple3<>(1, 8, 3000l),
			new Tuple3<>(1, 9, 3000l),

			new Tuple3<>(1, 10, 4000l),
			new Tuple3<>(10, 11, 4000l),
			new Tuple3<>(11, 12, 4000l),

			new Tuple3<>(1, 13, 5000l),
			new Tuple3<>(14, 15, 5000l),
			new Tuple3<>(17, 14, 5000l),
			new Tuple3<>(16, 13, 5000l)
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

	private static class SSSPSampleSrc extends RichSourceFunction<Edge<Integer, NullValue>> {
		
		@Override
		public void run(SourceContext<Edge<Integer, NullValue>> ctx) throws Exception {
			long curTime = -1;
			for (Tuple3<Integer, Integer, Long> next: sampleStream) {
				ctx.collectWithTimestamp(new Edge<>(next.f0, next.f1, NullValue.getInstance()), next.f2);

				if(curTime == -1){
					curTime = next.f2;
				}
				if(curTime < next.f2){
					curTime = next.f2;
					ctx.emitWatermark(new Watermark(curTime - 1));
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
			env.setParallelism(2);
			return new SimpleEdgeStream<>(env.addSource(new SSSPSampleSrc()), env);
		}
	}
	
}
