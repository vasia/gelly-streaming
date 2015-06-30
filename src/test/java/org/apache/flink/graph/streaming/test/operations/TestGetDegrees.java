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
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.test.GraphStreamTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestGetDegrees extends MultipleProgramsTestBase {

	public TestGetDegrees(TestExecutionMode mode) {
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

	@Test
	public void testGetDegrees() throws Exception {
		/*
		 * Test getDegrees() with the sample graph
	     */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		GraphStream<Long, Long> graph =
				new GraphStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

		graph.getDegrees().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		expectedResult = "1,1\n" +
				"1,2\n" +
				"1,3\n" +
				"2,1\n" +
				"2,2\n" +
				"3,1\n" +
				"3,2\n" +
				"3,3\n" +
				"3,4\n" +
				"4,1\n" +
				"4,2\n" +
				"5,1\n" +
				"5,2\n" +
				"5,3\n";
	}

	@Test
	public void testGetInDegrees() throws Exception {
		/*
		 * Test getInDegrees() with the sample graph
	     */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		GraphStream<Long, Long> graph =
				new GraphStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

		graph.getInDegrees().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		expectedResult = "1,1\n" +
				"2,1\n" +
				"3,1\n" +
				"3,2\n" +
				"4,1\n" +
				"5,1\n" +
				"5,2\n";
	}

	@Test
	public void testGetOutDegrees() throws Exception {
		/*
		 * Test getOutDegrees() with the sample graph
	     */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		GraphStream<Long, Long> graph =
				new GraphStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);

		graph.getOutDegrees().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		expectedResult = "1,1\n" +
				"1,2\n" +
				"2,1\n" +
				"3,1\n" +
				"3,2\n" +
				"4,1\n" +
				"5,1\n";
	}
}
