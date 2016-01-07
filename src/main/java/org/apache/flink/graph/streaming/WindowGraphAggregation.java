package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;


/**
 * WIP Graph Aggregation on Parallel Time Window
 *
 * @param <K>
 * @param <EV>
 * @param <S>
 * @param <T>
 */
public class WindowGraphAggregation<K, EV, S, T> extends GraphAggregation<K, EV, S, T> {

    private long timeMillis;


    public WindowGraphAggregation(FoldFunction<Edge<K, EV>, S> updateFun, ReduceFunction<S> combineFun, MapFunction<S,T> mergeFun, S initialVal, long timeMillis, boolean transientState) {
        super(updateFun, combineFun, mergeFun, initialVal, transientState);
        this.timeMillis = timeMillis;
    }
    
    public WindowGraphAggregation(FoldFunction<Edge<K, EV>, S> updateFun, ReduceFunction<S> combineFun, S initialVal, long timeMillis, boolean transientState) {
        this(updateFun,combineFun,null,initialVal,timeMillis,transientState);
    }

    @Override
    public DataStream<T> run(final DataStream<Edge<K, EV>> edgeStream) {

        //For parallel window support we key the edge stream by partition and apply a parallel fold per partition.
        //Finally, we merge all locally combined results into our final graph aggregation property 

        DataStream partialAgg = edgeStream.map(
                new RichMapFunction<Edge<K, EV>, Tuple2<Integer, Edge<K, EV>>>() {
                    @Override
                    public Tuple2<Integer, Edge<K, EV>> map(Edge<K, EV> edge) throws Exception {
                        return new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), edge);
                    }
                })
                .keyBy(0).timeWindow(Time.of(timeMillis, TimeUnit.MILLISECONDS))
                .fold(getInitialValue(), new FoldFunction<Tuple2<Integer, Edge<K, EV>>, S>() {
                    @Override
                    public S fold(S s, Tuple2<Integer, Edge<K, EV>> o) throws Exception {
                        return getUpdateFun().fold(s, o.f1);
                    }
                }).flatMap(getAggregator(edgeStream)).setParallelism(1);

        if (getMergeFun() != null) {
            return partialAgg.map(getMergeFun());
        }

        return partialAgg;
    }

}
