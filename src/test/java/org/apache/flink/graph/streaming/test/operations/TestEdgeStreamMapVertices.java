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

import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestEdgeStreamMapVertices extends MultipleProgramsTestBase {

	public TestEdgeStreamMapVertices(TestExecutionMode mode) {
		super(mode);
	}

	private String resultPath;
	private String expectedResult;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	/*
	@Test
	public void testWithSameType() throws Exception {
		// Test mapVertices() keeping the same vertex types
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		EdgeStream<Long, Long, Long> graph = EdgeStream.fromDataStream(
				GraphStreamTestUtils.getLongLongVertexDataStream(env),
				GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

		graph = graph.mapVertices(new AddOneMapper());

		graph.getVertices().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		expectedResult = "1,2\n" +
				"2,3\n" +
				"3,4\n" +
				"4,5\n" +
				"5,6\n";
	}

	private static final class AddOneMapper implements MapFunction<Vertex<Long, Long>, Long> {
		@Override
		public Long map(Vertex<Long, Long> vertex) throws Exception {
			return vertex.getValue() + 1;
		}
	}

	@Test
	public void testWithTupleType() throws Exception {
		// Test mapVertices() converting the vertex value type to tuple
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		EdgeStream<Long, Long, Long> graph = EdgeStream.fromDataStream(
				GraphStreamTestUtils.getLongLongVertexDataStream(env),
				GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

		EdgeStream<Long, Tuple2<Long, Long>, Long> mappedGraph = graph.mapVertices(new ToTuple2Mapper());

		mappedGraph.getVertices().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		expectedResult = "1,(1,2)\n" +
				"2,(2,3)\n" +
				"3,(3,4)\n" +
				"4,(4,5)\n" +
				"5,(5,6)\n";
	}

	private static final class ToTuple2Mapper implements MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) throws Exception {
			return new Tuple2<>(vertex.getValue(), vertex.getValue() + 1);
		}
	}

	@Test
	public void testChainedMaps() throws Exception {
		// Test mapVertices() where two maps are chained together
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		EdgeStream<Long, Long, Long> graph = EdgeStream.fromDataStream(
				GraphStreamTestUtils.getLongLongVertexDataStream(env),
				GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

		EdgeStream<Long, Tuple2<Long, Long>, Long> mappedGraph = graph
				.mapVertices(new AddOneMapper())
				.mapVertices(new ToTuple2Mapper());

		mappedGraph.getVertices().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		expectedResult = "1,(2,3)\n" +
				"2,(3,4)\n" +
				"3,(4,5)\n" +
				"4,(5,6)\n" +
				"5,(6,7)\n";
	}
	*/
}
