package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * @param <K>  key type
 * @param <EV> edge value type
 * @param <S>  intermediate state type
 * @param <T>  fold result type
 */
public abstract class GraphAggregation<K, EV, S extends Serializable, T> implements Serializable {

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
    private final MapFunction<S, T> trasform;

    private final S initialValue;

    /**
     * This flag indicates whether a merger state is cleaned up after an operation
     */
    private final boolean transientState;

    protected GraphAggregation(EdgesFold<K, EV, S> updateFun, ReduceFunction<S> combineFun, MapFunction<S, T> trasform, S initialValue, boolean transientState) {
        this.updateFun = updateFun;
        this.combineFun = combineFun;
        this.trasform = trasform;
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

    public MapFunction<S, T> getTrasform() {
        return trasform;
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
	private final static class Merger<S> extends WrappingFunction<ReduceFunction<S>> implements FlatMapFunction<S, S> {

        private final S initialVal;
        private S currentState;
        private final boolean transientState;

        private Merger(S initialVal, ReduceFunction<S> combiner, boolean transientState) {
            super(combiner);
            this.initialVal = initialVal;
            this.currentState = initialVal;
            this.transientState = transientState;
        }

        @Override
        public void flatMap(S s, Collector<S> collector) throws Exception {

            if (getWrappedFunction() != null) {
                currentState = getWrappedFunction().reduce(s, currentState);
                collector.collect(currentState);

                if (transientState) {
                    currentState = initialVal;
                }
            } else {
                collector.collect(s);
            }
        }
    }
}
