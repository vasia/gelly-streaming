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


package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;


/**
 * Graph Tree Aggregation on Parallel Time Window
 * 
 * TODO add documentation
 * 
 */
public class SummaryTreeReduce<K, EV, S extends Serializable, T> extends SummaryBulkAggregation<K, EV, S, T> {

	private static final long serialVersionUID = 1L;
	private int degree;
	

	public SummaryTreeReduce(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, MapFunction<S, T> transformFun, S initialVal, long timeMillis, boolean transientState, int degree) {
		super(updateFun, combineFun, transformFun, initialVal, timeMillis, transientState);
		this.degree = degree;
	}

	public SummaryTreeReduce(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, S initialVal, long timeMillis, boolean transientState, int degree) {
		this(updateFun, combineFun, null, initialVal, timeMillis, transientState, degree);
	}

	public SummaryTreeReduce(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, S initialVal, long timeMillis, boolean transientState) {
		this(updateFun, combineFun, null, initialVal, timeMillis, transientState, -1);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataStream<T> run(final DataStream<Edge<K, EV>> edgeStream) {
		TypeInformation<Tuple2<Integer, Edge<K, EV>>> basicTypeInfo = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, edgeStream.getType());

		TupleTypeInfo edgeTypeInfo = (TupleTypeInfo) edgeStream.getType();
		TypeInformation<S> partialAggType = TypeExtractor.createTypeInfo(EdgesFold.class, getUpdateFun().getClass(), 2, edgeTypeInfo.getTypeAt(0), edgeTypeInfo.getTypeAt(2));
		TypeInformation<Tuple2<Integer, S>> partialTypeInfo = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, partialAggType);

		degree = (degree == -1) ? edgeStream.getParallelism() : degree;
		
		DataStream<S> partialAgg = edgeStream
				.map(new PartitionMapper<>()).returns(basicTypeInfo)
				.setParallelism(degree)
				.keyBy(0)
				.timeWindow(Time.of(timeMillis, TimeUnit.MILLISECONDS))
				.fold(getInitialValue(), new PartialAgg<>(getUpdateFun(), partialAggType)).setParallelism(degree);
		//split here

		DataStream<Tuple2<Integer, S>> treeAgg = enhance(partialAgg.map(new PartitionMapper<>()).setParallelism(degree).returns(partialTypeInfo), partialTypeInfo);

		DataStream<S> resultStream = treeAgg.map(new PartitionStripper<>()).setParallelism(treeAgg.getParallelism())
				.timeWindowAll(Time.of(timeMillis, TimeUnit.MILLISECONDS))
				.reduce(getCombineFun())
				.flatMap(getAggregator(edgeStream)).setParallelism(1);

		return (getTransform() != null) ? resultStream.map(getTransform()) : (DataStream<T>) resultStream;
	}

	private DataStream<Tuple2<Integer, S>> enhance(DataStream<Tuple2<Integer, S>> input, TypeInformation<Tuple2<Integer, S>> aggType) {

		if (input.getParallelism() <= 2) {
			return input;
		}

		int nextParal = input.getParallelism() / 2;
		DataStream<Tuple2<Integer, S>> unpartitionedStream =
				input.keyBy(new KeySelector<Tuple2<Integer, S>, Integer>() {
					//collapse two partitions into one
					@Override
					public Integer getKey(Tuple2<Integer, S> record) throws Exception {
						return record.f0 / 2;
					}
				});

		//repartition stream to p / 2 aggregators
		KeyedStream<Tuple2<Integer, S>, Integer> repartitionedStream =
				unpartitionedStream.map(new PartitionReMapper()).returns(aggType)
						.setParallelism(nextParal)
						.keyBy(0);

		//window again on event time and aggregate
		DataStream<Tuple2<Integer, S>> aggregatedStream =
				repartitionedStream.timeWindow(Time.of(timeMillis, TimeUnit.MILLISECONDS))
						.reduce(new AggregationWrapper<>(getCombineFun()))       
						.setParallelism(nextParal);
		return enhance(aggregatedStream, aggType);
	}

	protected static final class PartitionReMapper<Y> extends RichMapFunction<Tuple2<Integer, Y>, Tuple2<Integer, Y>> {

		private int partitionIndex;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public Tuple2<Integer, Y> map(Tuple2<Integer, Y> tpl) throws Exception {
			return new Tuple2<>(partitionIndex, tpl.f1);
		}
	}

	public static class PartitionStripper<S> implements MapFunction<Tuple2<Integer, S>, S> {
		@Override
		public S map(Tuple2<Integer, S> tpl) throws Exception {
			return tpl.f1;
		}
	}

	public static class AggregationWrapper<S> implements ReduceFunction<Tuple2<Integer, S>> {

		private final ReduceFunction<S> wrappedFunction;

		protected AggregationWrapper(ReduceFunction<S> wrappedFunction) {
			this.wrappedFunction = wrappedFunction;
		}

		@Override
		public Tuple2<Integer, S> reduce(Tuple2<Integer, S> tpl1, Tuple2<Integer, S> tpl2) throws Exception {
			return new Tuple2<>(tpl1.f0, wrappedFunction.reduce(tpl1.f1, tpl2.f1));
		}
	}
}



	

