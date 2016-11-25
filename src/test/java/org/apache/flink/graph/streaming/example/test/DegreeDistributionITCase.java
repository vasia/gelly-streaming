/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.graph.streaming.example.DegreeDistribution;
import org.apache.flink.graph.streaming.util.ExamplesTestData;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

public class DegreeDistributionITCase extends StreamingProgramTestBase {
	
	protected String textPath;
	protected String resultPath;
	protected String textPath2;
	protected String resultPath2;

	@Override
	protected void preSubmit() throws Exception {
		setParallelism(1);
		textPath = createTempFile("edges.txt", ExamplesTestData.DEGREES_DATA);
		resultPath = getTempDirPath("result");
		textPath2 = createTempFile("edges2.txt", ExamplesTestData.DEGREES_DATA_ZERO);
		resultPath2 = getTempDirPath("result2");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(ExamplesTestData.DEGREES_RESULT, resultPath);
		compareResultsByLinesInMemory(ExamplesTestData.DEGREES_RESULT_ZERO, resultPath2);
	}

	@Override
	protected void testProgram() throws Exception {
		DegreeDistribution.main(new String[]{textPath, resultPath});
		DegreeDistribution.main(new String[]{textPath2, resultPath2});
	}
}