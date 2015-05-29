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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.EdgeOnlyStream;
import org.apache.flink.graph.streaming.example.utils.Degrees;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class DegreeCountAggregateExample {

	private int p;

	public DegreeCountAggregateExample(int p) throws Exception {
		this.p = p;
	}

	public long run() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setParallelism(this.p);

		// Source: http://grouplens.org/datasets/movielens/
		DataStream<Edge<Long, NullValue>> edges = env
				.readTextFile("movielens_tmp_sorted.txt")
				.map(new MapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(String s) throws Exception {
						String[] args = s.split(",");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						return new Edge<>(src, trg, NullValue.getInstance());
					}
				});

		EdgeOnlyStream<Long, NullValue> graph = new EdgeOnlyStream<>(edges, env);
		graph.getDegrees();

		env.getConfig().disableSysoutLogging();
		JobExecutionResult res = env.execute("EdgeOnlyStream.getDegrees() Measurements");

		return res.getNetRuntime();
	}

	public static void main(String[] args) throws Exception {

		DegreeCountAggregateExample dc = new DegreeCountAggregateExample(4);
		long res = dc.run();
		System.out.println("Result: " + res);

		/*
		String p4ResultFile = "d:\\_work\\gelly-streaming\\degree_count_results_p4.csv";
		String p2ResultFile = "d:\\_work\\gelly-streaming\\degree_count_results_p2.csv";
		String p1ResultFile = "d:\\_work\\gelly-streaming\\degree_count_results_p1.csv";

		// Measure different sizes
		for (int size = 1000000; size <= 20000000; size += 200000) {
			System.out.printf("Processing size %d\n", size);

			// Create new data set;
			String inFile = "d:\\_work\\gelly-streaming\\movielens_20m_sorted.txt";
			String outFile = "d:\\_work\\gelly-streaming\\movielens_tmp_sorted.txt";
			FileWriter outFileWriter = new FileWriter(outFile, false);

			try (BufferedReader br = new BufferedReader(new FileReader(inFile))) {
				String line;
				int count = 0;
				while (count < size && (line = br.readLine()) != null) {
					// process the line.
					outFileWriter.write(line);
					outFileWriter.write("\r\n");
					count++;
				}
			}
			outFileWriter.close();

			// Run p=4 version
			DegreeCountAggregateExample dc = new DegreeCountAggregateExample(4);
			long timeResult = dc.run();

			FileWriter res = new FileWriter(p4ResultFile, true);
			res.append(String.format("%d\r\n", timeResult));
			res.close();

			// Run p=2 version
			dc = new DegreeCountAggregateExample(2);
			timeResult = dc.run();

			res = new FileWriter(p2ResultFile, true);
			res.append(String.format("%d\r\n", timeResult));
			res.close();

			// Run centralized version
			dc = new DegreeCountAggregateExample(1);
			timeResult = dc.run();

			res = new FileWriter(p1ResultFile, true);
			res.append(String.format("%d\r\n", timeResult));
			res.close();
		}
		*/
	}
}
