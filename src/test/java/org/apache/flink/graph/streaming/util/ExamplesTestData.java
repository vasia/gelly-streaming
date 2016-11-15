/* Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.flink.graph.streaming.util;

public class ExamplesTestData {

	public static final String TRIANGLES_DATA =
				"1 2 100\n" + "1 3 150\n"
				+ "3 2 200\n" + "2 4 250\n"
				+ "3 4 300\n" + "3 5 350\n"
				+ "4 5 400\n" + "4 6 450\n"
				+ "6 5 500\n" + "5 7 550\n"
				+ "6 7 600\n" + "8 6 650\n"
				+ "7 8 700\n" + "7 9 750\n"
				+ "8 9 800\n" + "10 8 850\n"
				+ "9 10 900\n" + "9 11 950\n"
				+ "10 11 1000";

	public static final String TRIANGLES_RESULT =
			"(2,1199)\n(2,399)\n(3,799)\n";

	public static final String DEGREES_DATA =
			"1 2 +\n" + "2 3 +\n" + "1 4 +\n"
			+ "2 3 -\n" + "3 4 +\n" + "1 2 -";

	public static final String DEGREES_RESULT =
			"(1,1)\n(1,2)\n" +
					"(2,1)\n(1,1)\n(1,2)\n" +
					"(2,2)\n(1,1)\n(1,2)\n" +
					"(1,3)\n(2,1)\n(1,2)\n"+
					"(1,3)\n(2,2)\n(1,2)\n" +
					"(1,3)\n(2,1)\n(1,2)";

	public static final String DEGREES_DATA_ZERO =
			"1 2 +\n" + "2 3 +\n" + "1 4 +\n" +
					"2 3 -\n" + "3 4 +\n" + "1 2 -\n" +
					"2 3 -";

	public static final String DEGREES_RESULT_ZERO =
			"(1,1)\n(1,2)\n" +
					"(2,1)\n(1,1)\n(1,2)\n" +
					"(2,2)\n(1,1)\n(1,2)\n" +
					"(1,3)\n(2,1)\n(1,2)\n"+
					"(1,3)\n(2,2)\n(1,2)\n" +
					"(1,3)\n(2,1)\n(1,2)\n" +
					"(1,1)";
}