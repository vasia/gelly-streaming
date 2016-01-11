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
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 */
public class ConnectedComponentsGlobal implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        GraphStream<Long, NullValue, Long> edges = getTestGraphStream(env);

        DataStream<DisjointSet<Long>> cc = edges.aggregate(
                new WindowGraphAggregation<Long, Long, DisjointSet<Long>, DisjointSet<Long>>(
                        new UpdateCC(), new CombineCC(), new DisjointSet<Long>(), 500, false));
        cc.print().setParallelism(1);
        env.execute("Streaming Connected Components");
    }

    @SuppressWarnings("serial")
    public static class UpdateCC implements FoldFunction<Edge<Long, Long>, DisjointSet<Long>> {

        @Override
        public DisjointSet<Long> fold(DisjointSet<Long> ds, Edge<Long, Long> o) throws Exception {
            ds.union(o.f0, o.f1);
            return ds;
        }
    }

    @SuppressWarnings("serial")
    private static class CombineCC implements ReduceFunction<DisjointSet<Long>> {
        @Override
        public DisjointSet<Long> reduce(DisjointSet<Long> s1, DisjointSet<Long> s2) throws Exception {
            int count1 = s1.getMatches().size();
            int count2 = s2.getMatches().size();
            if (count1 <= count2) {
                s2.merge(s1);
                return s2;
            }

            s1.merge(s2);
            return s1;
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************


    private static GraphStream<Long, NullValue, Long> getTestGraphStream(StreamExecutionEnvironment env) {

        return new SimpleEdgeStream<>(env.generateSequence(1, 100).flatMap(
                new FlatMapFunction<Long, Edge<Long, Long>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, Long>> out) throws Exception {
                        out.collect(new Edge<>(key, key + 2, key * 100));
                    }
                }),
                new AscendingTimestampExtractor<Edge<Long, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Edge<Long, Long> element, long currentTimestamp) {
                        return element.getValue();
                    }
                }, env);
    }

    @Override
    public String getDescription() {
        return "Streaming Connected Components on Global Aggregation";
    }


}
