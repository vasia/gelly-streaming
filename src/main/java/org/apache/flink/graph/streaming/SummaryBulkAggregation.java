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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;


/**
 * Graph Aggregation on Parallel Time Window
 *
 * TODO add documentation
 * 
 * @param <K> the edge stream's key type
 * @param <EV> the edges stream's value type
 * @param <S> the output type of the partial aggregation
 * @param <T> the output type of the result
 */
public class SummaryBulkAggregation<K, EV, S extends Serializable, T> extends SummaryAggregation<K, EV, S, T> {

	private static final long serialVersionUID = 1L;
	protected long timeMillis;


	public SummaryBulkAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, MapFunction<S, T> transformFun, S initialVal, long timeMillis, boolean transientState) {
		super(updateFun, combineFun, transformFun, initialVal, transientState);
		this.timeMillis = timeMillis;
	}

	public SummaryBulkAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, S initialVal, long timeMillis, boolean transientState) {
		this(updateFun, combineFun, null, initialVal, timeMillis, transientState);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataStream<T> run(final DataStream<Edge<K, EV>> edgeStream) {

		//For parallel window support we key the edge stream by partition and apply a parallel fold per partition.
		//Finally, we merge all locally combined results into our final graph aggregation property.
		TupleTypeInfo edgeTypeInfo = (TupleTypeInfo) edgeStream.getType();
		TypeInformation<S> returnType = TypeExtractor.createTypeInfo(EdgesFold.class, getUpdateFun().getClass(), 2, edgeTypeInfo.getTypeAt(0), edgeTypeInfo.getTypeAt(2));

		TypeInformation<Tuple2<Integer, Edge<K, EV>>> typeInfo = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, edgeStream.getType());
		DataStream<S> partialAgg = edgeStream
				.map(new PartitionMapper<>()).returns(typeInfo)
				.keyBy(0)
				.timeWindow(Time.of(timeMillis, TimeUnit.MILLISECONDS))
				.fold(getInitialValue(), new PartialAgg<>(getUpdateFun(),returnType))
				.timeWindowAll(Time.of(timeMillis, TimeUnit.MILLISECONDS))
				.reduce(getCombineFun())
				.flatMap(getAggregator(edgeStream)).setParallelism(1);

		if (getTransform() != null) {
			return partialAgg.map(getTransform());
		}

		return (DataStream<T>) partialAgg;
	}

	@SuppressWarnings("serial")
	protected static final class PartitionMapper<Y> extends RichMapFunction<Y, Tuple2<Integer, Y>> {

		private int partitionIndex;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public Tuple2<Integer, Y> map(Y state) throws Exception {
			return new Tuple2<>(partitionIndex, state);
		}
	}

	@SuppressWarnings("serial")
	protected static final class PartialAgg<K, EV, S>
			implements ResultTypeQueryable<S>, FoldFunction<Tuple2<Integer, Edge<K, EV>>, S> {

		private EdgesFold<K, EV, S> foldFunction;
		private TypeInformation<S> returnType;

		public PartialAgg(EdgesFold<K, EV, S> foldFunction, TypeInformation<S> returnType) {
			this.foldFunction = foldFunction;
			this.returnType = returnType;
		}

		@Override
		public S fold(S s, Tuple2<Integer, Edge<K, EV>> o) throws Exception {
			return this.foldFunction.foldEdges(s, o.f1.getSource(), o.f1.getTarget(), o.f1.getValue());
		}

		@Override
		public TypeInformation<S> getProducedType() {
			return returnType;
		}

	}
}

