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
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * The Connected Components algorithm assigns a component ID to each vertex in the graph.
 * Vertices that belong to the same component have the same component ID.
 * This algorithm computes _weakly_ connected components, i.e. edge direction is ignored.
 */
public class ConnectedComponentsGlobal implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        GraphStream<Long, NullValue, NullValue> edges = getEdgesDataSet(env);
        DataStream<DisjointSet<Long>> cc = edges.aggregate(
                new WindowGraphAggregation<Long, NullValue, DisjointSet<Long>, DisjointSet<Long>>(
                        new UpdateCC(), new CombineCC(), new DisjointSet<Long>(), 500, false));

        cc.flatMap(new FlattenCC()).print();
        env.execute("Streaming Connected Components");
    }


    @SuppressWarnings("serial")
	public static class UpdateCC implements FoldFunction<Edge<Long, NullValue>, DisjointSet<Long>> {

        @Override
        public DisjointSet<Long> fold(DisjointSet<Long> longDisjointSet, Edge<Long, NullValue> o) throws Exception {
            longDisjointSet.union(o.f0, o.f1);
            return longDisjointSet;
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
    
    @SuppressWarnings("serial")
	private static class FlattenCC implements FlatMapFunction<DisjointSet<Long>, Tuple2<Long, Long>>{

        @Override
        public void flatMap(DisjointSet<Long> components, Collector<Tuple2<Long, Long>> collector) throws Exception {
            for(Long vertex : components.getMatches().keySet()){
                collector.collect(new Tuple2<>(vertex, components.find(vertex)));
            }
        }
    } 

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String edgeInputPath = null;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 1) {
                System.err.println("Usage: ConnectedComponents <input edges path>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
        } else {
            System.out.println("Executing ConnectedComponents example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: ConnectedComponents <input edges path>");
        }
        return true;
    }

    @SuppressWarnings("serial")
	private static GraphStream<Long, NullValue, NullValue> getEdgesDataSet(StreamExecutionEnvironment env) {

        if (fileOutput) {
            return new SimpleEdgeStream<>(env.readTextFile(edgeInputPath)
                    .map(new MapFunction<String, Edge<Long, NullValue>>() {
                        @Override
                        public Edge<Long, NullValue> map(String s) {
                            String[] fields = s.split("\\t");
                            long src = Long.parseLong(fields[0]);
                            long trg = Long.parseLong(fields[1]);
                            return new Edge<>(src, trg, NullValue.getInstance());
                        }
                    }), env);
        }

        return new SimpleEdgeStream<>(env.generateSequence(1, 10).flatMap(
                new FlatMapFunction<Long, Edge<Long, NullValue>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) throws Exception {
                        for (int i = 1; i < 3; i++) {
                            long target = key + i;
                            out.collect(new Edge<>(key, target, NullValue.getInstance()));
                        }
                    }
                }), env);
    }

    @Override
    public String getDescription() {
        return "Streaming Connected Components on Global Aggregation";
    }


}
