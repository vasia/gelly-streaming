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
package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.ExactTriangleCount;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class TriangleCountTest {

    // first flatMap collector and result Tuple
    private Collector<Tuple3<Integer, Integer, TreeSet<Integer>>> out1;
    private Tuple3<Integer, Integer, TreeSet<Integer>> resultTuple1;

    // second flatMap collector and result List
    private Collector<Tuple2<Integer, Integer>> out2;
    private List<Tuple2<Integer, Integer>> resultList2;

    // third flatMap collector and result Tuple
    private Collector<Tuple2<Integer, Integer>> out3;
    private Tuple2<Integer, Integer> resultTuple3;

    @Before
    public void setUp() throws Exception {
        out1 = new Collector<Tuple3<Integer, Integer, TreeSet<Integer>>>() {
            @Override
            public void collect(Tuple3<Integer, Integer, TreeSet<Integer>> t) {
                resultTuple1 = t;
            }

            @Override
            public void close() {
                ;
            }
        };

        resultList2 = new ArrayList<>();
        out2 = new Collector<Tuple2<Integer, Integer>>() {
            @Override
            public void collect(Tuple2<Integer, Integer> t) {
                resultList2.add(t);
            }

            @Override
            public void close() {
                resultList2.clear();
            }
        };

        out3 = new Collector<Tuple2<Integer, Integer>>() {
            @Override
            public void collect(Tuple2<Integer, Integer> t) {
                resultTuple3 = t;
            }

            @Override
            public void close() {
                ;
            }
        };
    }

    @Test
    public void testIntersection() throws Exception {
        FlatMapFunction f = new ExactTriangleCount.IntersectNeighborhoods();

        TreeSet<Integer> t1 = new TreeSet<>();
        t1.add(2);
        t1.add(3);
        t1.add(5);
        t1.add(7);
        t1.add(9);
        Tuple3<Integer, Integer, TreeSet<Integer>> input1 = new Tuple3<>(1, 2, t1);

        Tuple3<Integer, Integer, TreeSet<Integer>> input2 = new Tuple3<>(1, 3, t1);
        TreeSet<Integer> t2 = new TreeSet<>();
        t2.add(1);
        t2.add(3);
        t2.add(4);
        t2.add(5);
        t2.add(15);
        t2.add(18);

        Tuple3<Integer, Integer, TreeSet<Integer>> input3 = new Tuple3<>(1, 2, t2);

        f.flatMap(input1, out2);
        Assert.assertEquals(0, resultList2.size());

        f.flatMap(input2, out2);
        Assert.assertEquals(0, resultList2.size());

        f.flatMap(input3, out2);
        Assert.assertEquals(5, resultList2.size());
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(3, 1)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(5, 1)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(1, 2)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(2, 2)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(-1, 2)));

        TreeSet<Integer> t3 = new TreeSet<>();
        t3.add(1);
        t3.add(2);
        t3.add(7);
        t3.add(8);
        Tuple3<Integer, Integer, TreeSet<Integer>> input4 = new Tuple3<>(1, 3, t3);

        resultList2.clear();
        f.flatMap(input4, out2);
        Assert.assertEquals(5, resultList2.size());
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(2, 1)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(7, 1)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(1, 2)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(3, 2)));
        Assert.assertEquals(true, resultList2.contains(new Tuple2<>(-1, 2)));
    }

    @Test
    public void testCounts() throws Exception {
        FlatMapFunction f = new ExactTriangleCount.SumAndEmitCounters();
        Tuple2<Integer, Integer> expected = new Tuple2<>();

        f.flatMap(new Tuple2<>(-1, 1), out3);
        expected.setField(-1, 0);
        expected.setField(1, 1);
        Assert.assertEquals(expected, resultTuple3);

        f.flatMap(new Tuple2<>(-1, 5), out3);
        expected.setField(-1, 0);
        expected.setField(6, 1);
        Assert.assertEquals(expected, resultTuple3);

        f.flatMap(new Tuple2<>(2, 2), out3);
        expected.setField(2, 0);
        expected.setField(2, 1);
        Assert.assertEquals(expected, resultTuple3);

        f.flatMap(new Tuple2<>(-1, 4), out3);
        expected.setField(-1, 0);
        expected.setField(10, 1);
        Assert.assertEquals(expected, resultTuple3);

        f.flatMap(new Tuple2<>(2, 4), out3);
        expected.setField(2, 0);
        expected.setField(6, 1);
        Assert.assertEquals(expected, resultTuple3);
    }
}
