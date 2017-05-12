package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * @param <K>  key type
 * @param <EV> edge value type
 * @param <S>  intermediate state type
 * @param <T>  fold result type
 */
public abstract class SummaryAggregation<K, EV, S extends Serializable, T> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
     * A function applied to each edge in an edge stream that aggregates a user-defined graph property state. In case
     * we slice the edge stream into windows a fold will output its aggregation state value per window, otherwise, this
     * operates edge-wise
     */
    private final EdgesFold<K, EV, S> updateFun;

    /**
     * An optional combine function for updating graph property state
     */
    private final ReduceFunction<S> combineFun;

    /**
     * An optional map function that converts state to output
     */
    private final MapFunction<S, T> transform;

    private final S initialValue;

    /**
     * This flag indicates whether a merger state is cleaned up after an operation
     */
    private final boolean transientState;

    protected SummaryAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, MapFunction<S, T> transform, S initialValue, boolean transientState) {
        this.updateFun = updateFun;
        this.combineFun = combineFun;
        this.transform = transform;
        this.initialValue = initialValue;
        this.transientState = transientState;
    }


    public abstract DataStream<T> run(DataStream<Edge<K, EV>> edgeStream);


    public ReduceFunction<S> getCombineFun() {
        return combineFun;
    }

    public EdgesFold<K, EV, S> getUpdateFun() {
        return updateFun;
    }

    public MapFunction<S, T> getTransform() {
        return transform;
    }

    public boolean isTransientState() {
        return transientState;
    }

    public S getInitialValue() {
        return initialValue;
    }

    //FIXME - naive prototype - blocking reduce should be implemented correctly
    protected FlatMapFunction<S, S> getAggregator(final DataStream<Edge<K, EV>> edgeStream) {
        return new Merger<>(getInitialValue(), getCombineFun(), isTransientState());
    }

    /**
     * In this prototype the Merger is non-blocking and merges partitions incrementally
     *
     * @param <S>
     */
    @SuppressWarnings("serial")
	private final static class Merger<S extends Serializable> extends WrappingFunction<ReduceFunction<S>> implements FlatMapFunction<S, S>, ListCheckpointed<S> {

        private final S initialVal;
        private S summary;
        private final boolean transientState;

        private Merger(S initialVal, ReduceFunction<S> combiner, boolean transientState) {
            super(combiner);
            this.initialVal = initialVal;
            this.summary = initialVal;
            this.transientState = transientState;
        }

        @Override
        public void flatMap(S s, Collector<S> collector) throws Exception {

            if (getWrappedFunction() != null) {
                summary = getWrappedFunction().reduce(s, summary);
                collector.collect(summary);

                if (transientState) {
                    summary = initialVal;
                }
            } else {
                collector.collect(s);
            }
        }

        /**
         * Graph state is task-parallel, thus, we use operator state.
         * 
         * TODO In the future we can change the redistribution strategy to split a summary and repartition it customly
         *
         */
        @Override
        public List<S> snapshotState(long l, long l1) throws Exception {
            return Collections.singletonList(summary);
        }

        @Override
        public void restoreState(List<S> list) throws Exception {
            summary = list.get(0);
        }
    }
}
