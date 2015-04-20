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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class TopDegreesExample {

	public TopDegreesExample() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		final Random rnd = new Random(0xDEADBEEF);

		DataStream<Edge<Long, NullValue>> edges = env.generateSequence(0, 100)
				.flatMap(new FlatMapFunction<Long, Edge<Long, NullValue>>() {
					@Override
					public void flatMap(Long value, Collector<Edge<Long, NullValue>> out) throws Exception {
						for (int i = 0; i < rnd.nextInt(100); ++i) {
							out.collect(new Edge<>(value, (value + rnd.nextInt(100)) % 100, NullValue.getInstance()));
						}
					}
				});

		edges
				.flatMap(new InitialDegreeMapper(InitialDegreeMapper.Direction.ALL))
				.flatMap(new TopDegreeSumMapper())
				.print();

		env.execute("Top Degrees Example");
	}

	private static final class TopDegreeSumMapper
			implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private Map<Long, Long> degrees;
		private long topDegree;

		public TopDegreeSumMapper() {
			degrees = new HashMap<>();
			topDegree = -1L;
		}

		@Override
		public void flatMap(Tuple2<Long, Long> degree, Collector<Tuple2<Long, Long>> out) throws Exception {

			long vertex = degree.f0;
			long count = degree.f1;

			// Update the degree map
			if (!degrees.containsKey(vertex)) {
				degrees.put(vertex, 0L);
			}

			degrees.put(vertex, degrees.get(vertex) + count);

			// Check if there is a new top degree
			if (!degrees.containsKey(topDegree) || degrees.get(topDegree) < degrees.get(vertex)) {
				topDegree = vertex;
				out.collect(new Tuple2<>(topDegree, degrees.get(topDegree)));
			}
		}
	}

	private static final class InitialDegreeMapper
			implements FlatMapFunction<Edge<Long,NullValue>, Tuple2<Long, Long>> {

		public enum Direction {IN, OUT, ALL};
		private Direction direction;

		public InitialDegreeMapper(Direction direction) {
			this.direction = direction;
		}

		@Override
		public void flatMap(Edge<Long, NullValue> edge, Collector<Tuple2<Long, Long>> out) throws Exception {

			if (direction.equals(Direction.OUT) || direction.equals(Direction.ALL)) {
				out.collect(new Tuple2<>(edge.getSource(), 1L));
			}
			if (direction.equals(Direction.IN) || direction.equals(Direction.ALL)) {
				out.collect(new Tuple2<>(edge.getTarget(), 1L));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		new TopDegreesExample();
	}
}

