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

package org.apache.flink.graph.streaming.test.operations;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.streaming.EdgesApply;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.EdgesReduce;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.test.GraphStreamTestUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class TestSlice extends AbstractTestBase {

    @Test
	public void testFoldNeighborsDefault() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,25\n" +
                "2,23\n" +
                "3,69\n" +
                "4,45\n" +
                "5,51\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, Long>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS))
			.foldNeighbors(new Tuple2<Long, Long>(0l, 0l), new SumEdgeValues());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testFoldNeighborsIn() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,51\n" +
                "2,12\n" +
                "3,36\n" +
                "4,34\n" +
                "5,80\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, Long>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS), EdgeDirection.IN)
			.foldNeighbors(new Tuple2<Long, Long>(0l, 0l), new SumEdgeValues());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testFoldNeighborsAll() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,76\n" +
                "2,35\n" +
                "3,105\n" +
                "4,79\n" +
                "5,131\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, Long>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS), EdgeDirection.ALL)
			.foldNeighbors(new Tuple2<Long, Long>(0l, 0l), new SumEdgeValues());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testReduceOnNeighborsDefault() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,25\n" +
                "2,23\n" +
                "3,69\n" +
                "4,45\n" +
                "5,51\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, Long>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS))
			.reduceOnEdges(new SumEdgeValuesReduce());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testReduceOnNeighborsIn() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,51\n" +
                "2,12\n" +
                "3,36\n" +
                "4,34\n" +
                "5,80\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, Long>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS), EdgeDirection.IN)
				.reduceOnEdges(new SumEdgeValuesReduce());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testReduceOnNeighborsAll() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,76\n" +
                "2,35\n" +
                "3,105\n" +
                "4,79\n" +
                "5,131\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, Long>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS), EdgeDirection.ALL)
				.reduceOnEdges(new SumEdgeValuesReduce());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testApplyOnNeighborsDefault() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,small\n" +
                "2,small\n" +
                "3,big\n" +
                "4,small\n" +
                "5,big\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, String>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS))
				.applyOnNeighbors(new SumEdgeValuesApply());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testApplyOnNeighborsIn() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,big\n" +
                "2,small\n" +
                "3,small\n" +
                "4,small\n" +
                "5,big\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, String>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS), EdgeDirection.IN)
				.applyOnNeighbors(new SumEdgeValuesApply());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testApplyOnNeighborsAll() throws Exception {
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,big\n" +
                "2,small\n" +
                "3,big\n" +
                "4,big\n" +
                "5,big\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		SimpleEdgeStream<Long, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		DataStream<Tuple2<Long, String>> sum = graph.slice(Time.of(1, TimeUnit.SECONDS), EdgeDirection.ALL)
				.applyOnNeighbors(new SumEdgeValuesApply());
		sum.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@SuppressWarnings("serial")
	private static final class SumEdgeValues implements EdgesFold<Long, Long, Tuple2<Long, Long>> {

		public Tuple2<Long, Long> foldEdges(Tuple2<Long, Long> accum, Long id, Long neighborID, Long edgeValue) {
			accum.setField(id, 0);
			accum.setField(accum.f1 + edgeValue, 1);
			return accum;
		}
	}

	@SuppressWarnings("serial")
	private static final class SumEdgeValuesReduce implements EdgesReduce<Long> {

		@Override
		public Long reduceEdges(Long firstEdgeValue, Long secondEdgeValue) {
			return firstEdgeValue + secondEdgeValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class SumEdgeValuesApply implements EdgesApply<Long, Long, Tuple2<Long, String>> {

		@Override
		public void applyOnEdges(Long vertexID,
				Iterable<Tuple2<Long, Long>> neighbors, Collector<Tuple2<Long, String>> out) {
			long sum = 0;
			for (Tuple2<Long, Long> n: neighbors) {
				sum += n.f1;
			}
			if (sum > 50) {
				out.collect(new Tuple2<Long, String>(vertexID, "big"));
			}
			else {
				out.collect(new Tuple2<Long, String>(vertexID, "small"));
			}
		}
	}
}
