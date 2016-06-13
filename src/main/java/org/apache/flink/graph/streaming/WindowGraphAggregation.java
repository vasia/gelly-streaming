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
 * WIP Graph Aggregation on Parallel Time Window
 *
 * @param <K> the edge stream's key type
 * @param <EV> the edges stream's value type
 * @param <S> the output type of the partial aggregation
 * @param <T> the output type of the result
 */
public class WindowGraphAggregation<K, EV, S extends Serializable, T> extends GraphAggregation<K, EV, S, T> {

	private static final long serialVersionUID = 1L;
	private long timeMillis;


	public WindowGraphAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, MapFunction<S, T> transformFun, S initialVal, long timeMillis, boolean transientState) {
		super(updateFun, combineFun, transformFun, initialVal, transientState);
		this.timeMillis = timeMillis;
	}

	public WindowGraphAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, S initialVal, long timeMillis, boolean transientState) {
		this(updateFun, combineFun, null, initialVal, timeMillis, transientState);
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataStream<T> run(final DataStream<Edge<K, EV>> edgeStream) {

		//For parallel window support we key the edge stream by partition and apply a parallel fold per partition.
		//Finally, we merge all locally combined results into our final graph aggregation property.


		TypeInformation<Tuple2<Integer, Edge<K, EV>>> typeInfo = new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, edgeStream.getType());
		DataStream<S> partialAgg = edgeStream
				.map(new InitialMapper<K, EV>()).returns(typeInfo)
				.keyBy(0)
				.timeWindow(Time.of(timeMillis, TimeUnit.MILLISECONDS))
				.fold(getInitialValue(), new PartialAgg<>(getUpdateFun())).flatMap(getAggregator(edgeStream)).setParallelism(1);

		if (getTrasform() != null) {
			return partialAgg.map(getTrasform());
		}

		return (DataStream<T>) partialAgg;
	}

	@SuppressWarnings("serial")
	private static final class InitialMapper<K, EV> extends RichMapFunction<Edge<K, EV>, Tuple2<Integer, Edge<K, EV>>> {

		private int partitionIndex;

		@Override
		public void open(Configuration parameters) throws Exception {
			this.partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
		}

		@Override
		public Tuple2<Integer, Edge<K, EV>> map(Edge<K, EV> edge) throws Exception {
			return new Tuple2<>(partitionIndex, edge);
		}
	}

	@SuppressWarnings("serial")
	private static final class PartialAgg<K, EV, S>
			implements ResultTypeQueryable<S>, FoldFunction<Tuple2<Integer, Edge<K, EV>>, S> {

		private EdgesFold<K, EV, S> foldFunction;

		public PartialAgg(EdgesFold<K, EV, S> foldFunction) {
			this.foldFunction = foldFunction;
		}

		@Override
		public S fold(S s, Tuple2<Integer, Edge<K, EV>> o) throws Exception {
			return this.foldFunction.foldEdges(s, o.f1.getSource(), o.f1.getTarget(), o.f1.getValue());
		}

		@Override
		public TypeInformation<S> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFold.class, this.foldFunction.getClass(), 2,
					null, null);
		}

	}
}

