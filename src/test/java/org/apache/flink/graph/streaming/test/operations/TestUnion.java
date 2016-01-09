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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

public class TestUnion extends StreamingProgramTestBase {

	private String resultPath;
	private String expectedResult;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Override
	protected void preSubmit() throws Exception {
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		     /*
		 * Test union() with two simple graphs
	     */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		List<Edge<Long, Long>> edgesA = new ArrayList<>();
		edgesA.add(new Edge<>(1L, 2L, 12L));
		edgesA.add(new Edge<>(1L, 3L, 13L));
		edgesA.add(new Edge<>(2L, 3L, 23L));
		edgesA.add(new Edge<>(3L, 4L, 34L));

		List<Edge<Long, Long>> edgesB = new ArrayList<>();
		edgesB.add(new Edge<>(3L, 5L, 35L));
		edgesB.add(new Edge<>(4L, 5L, 45L));
		edgesB.add(new Edge<>(5L, 1L, 51L));

		SimpleEdgeStream<Long, Long> graphA = new SimpleEdgeStream<>(env.fromCollection(edgesA), env);

		SimpleEdgeStream<Long, Long> graphB = new SimpleEdgeStream<>(env.fromCollection(edgesB), env);

		GraphStream<Long, NullValue, Long> graph = graphA.union(graphB);

		graph.getEdges()
				.writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);

		env.execute();
		expectedResult = "1,2,12\n" +
				"1,3,13\n" +
				"2,3,23\n" +
				"3,4,34\n" +
				"3,5,35\n" +
				"4,5,45\n" +
				"5,1,51\n";
	}

}
