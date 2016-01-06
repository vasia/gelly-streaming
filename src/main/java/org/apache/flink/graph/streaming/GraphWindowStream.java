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

package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A stream of discrete graphs, each maintaining
 * the graph state of the edges contained in the respective window.
 * It is created by calling {@link GraphStream#slice()}.
 *
 * @param <K> the vertex ID type
 * @param <EV> the edge value type
 */
public class GraphWindowStream<K, EV> {

	private WindowedStream<Edge<K, EV>, K, TimeWindow> windowedStream;

	GraphWindowStream(WindowedStream<Edge<K, EV>, K, TimeWindow> window) {
		this.windowedStream = window;
	}

	/**
	 * Performs a neighborhood aggregation on the graph window stream.
	 * 
	 * @param initialValue
	 * @param edgesReducer
	 * @return
	 */
	public <T> DataStream<T> reduceOnNeighbors(T initialValue, final EdgesFold<K, EV, T> edgesReducer) {
		return windowedStream.fold(initialValue, new EdgesFoldFunction<K, EV, T>(edgesReducer));
	}

	@SuppressWarnings("serial")
	public static final class EdgesFoldFunction<K, EV, T> implements FoldFunction<Edge<K, EV>, T>,
		ResultTypeQueryable<T>{

		private final EdgesFold<K, EV, T> edgesReducer;

		public EdgesFoldFunction(EdgesFold<K, EV, T> edgesReducer) {
			this.edgesReducer = edgesReducer;
		}

		@Override
		public T fold(T accumulator, Edge<K, EV> edge) throws Exception {
			return edgesReducer.reduceEdges(accumulator, edge.getSource(), edge.getTarget(), edge.getValue());
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFold.class, edgesReducer.getClass(), 2,
				null, null);
		}
	};

}
