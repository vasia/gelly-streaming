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

import java.util.*;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

/**
 * A stream of discrete graphs, each maintaining
 * the graph state of the edges contained in the respective window.
 * It is created by calling {@link SimpleEdgeStream#slice(Time)}.
 * The graph slice is keyed by the source or target vertex of the edge stream,
 * so that all edges of a vertex are in the same tumbling window.
 *
 * @param <K> the vertex ID type
 * @param <EV> the edge value type
 */
public class SnapshotStream<K, EV> {

	private WindowedStream<Edge<K, EV>, K, TimeWindow> rawSnapshotStream;
	private Time granularity;

	SnapshotStream(WindowedStream<Edge<K, EV>, K, TimeWindow> window, Time snapshotGranularity) {
		this.rawSnapshotStream = window;
		this.granularity = snapshotGranularity;
	}

	/**
	 * Performs a neighborhood fold on the graph window stream.
	 * 
	 * @param initialValue
	 * @param foldFunction
	 * @return the result stream after applying the user-defined fold operation on the window
	 */
	public <T> DataStream<T> foldNeighbors(T initialValue, final EdgesFold<K, EV, T> foldFunction) {
		return rawSnapshotStream.fold(initialValue, new EdgesFoldFunction<K, EV, T>(foldFunction));
	}

	@SuppressWarnings("serial")
	public static final class EdgesFoldFunction<K, EV, T>
			implements FoldFunction<Edge<K, EV>, T>, ResultTypeQueryable<T>
	{

		private final EdgesFold<K, EV, T> foldFunction;

		public EdgesFoldFunction(EdgesFold<K, EV, T> foldFunction) {
			this.foldFunction = foldFunction;
		}

		@Override
		public T fold(T accumulator, Edge<K, EV> edge) throws Exception {
			return foldFunction.foldEdges(accumulator, edge.getSource(), edge.getTarget(), edge.getValue());
		}

		@Override
		public TypeInformation<T> getProducedType() {
			return TypeExtractor.createTypeInfo(EdgesFold.class, foldFunction.getClass(), 2,
				null, null);
		}
	}

	/**
//	 * Performs an aggregation on the neighboring edges of each vertex on the graph window stream.
	 * <p>
	 * For each vertex, the transformation consecutively calls a
	 * {@link EdgesReduce} function until only a single value for each edge remains.
	 * The {@link EdgesReduce} function combines two edge values into one new value of the same type.
	 * 
	 * @param reduceFunction the aggregation function
	 * @return a result stream of Tuple2, containing one tuple per vertex.
	 * The first field is the vertex ID and the second field is the final value,
	 * after applying the user-defined aggregation operation on the neighborhood.
	 */
	public DataStream<Tuple2<K, EV>> reduceOnEdges(final EdgesReduce<EV> reduceFunction) {
		return rawSnapshotStream.reduce(new EdgesReduceFunction<K, EV>(reduceFunction))
			.project(0, 2);
	}

	@SuppressWarnings("serial")
	public static final class EdgesReduceFunction<K, EV> implements ReduceFunction<Edge<K, EV>> {

		private final EdgesReduce<EV> reduceFunction;

		public EdgesReduceFunction(EdgesReduce<EV> reduceFunction) {
			this.reduceFunction = reduceFunction;
		}

		@Override
		public Edge<K, EV> reduce(Edge<K, EV> firstEdge, Edge<K, EV> secondEdge) throws Exception {
			EV reducedValue = this.reduceFunction.reduceEdges(firstEdge.getValue(), secondEdge.getValue());
			firstEdge.setValue(reducedValue);
			return firstEdge;
		}
	}

	/**
	 * Executes an iterative computation scoped per Snapshot (edge stream window).
	 * In the current implementation a full graph snapshot is being created and kept internally while 
	 * iterative computation is synchronized across workers.
	 *
	 * Note: For completeness it is recommended to run numIterations on sliced graph streams with ALL direction.
	 * 
	 * TODO add support for fixpoint and stale synchronous
	 * 
	 * @param iterativeComputation
	 * @param <VV> The type of computational state in focus scoped per vertex (e.g., ranks, connected components etc.)
	 * @param <Message> The type of messages between vertices circulating within the iterative computation   
	 * @param <OUT> The type of the output stream triggered by the iterative computation
	 * @return
	 */
	public <VV, Message extends VertexMessage<K>, OUT> DataStream<OUT> run(GraphSnapshotIteration<K, VV, Message, OUT> iterativeComputation, int numIterations) throws Exception {
		
		//form adjacency lists to trigger iterative computation on that state


		TypeInformation<K> keyType = ((TupleTypeInfo<?>) rawSnapshotStream.getInputType()).getTypeAt(0);
		SingleOutputStreamOperator adjStream = 
				this.applyOnNeighbors(new EdgeFoldWrapper(keyType));
//						.returns(TypeInformation.of(new TypeHint<Tuple2<K, Iterable<K>>>() {
//					@Override
//					public TypeInformation<Tuple2<K, Iterable<K>>> getTypeInfo() {
//						return new TupleTypeInfo<>(keyType);
//					}
//				}));
		
		KeyedStream<Tuple2<K, Iterable<K>>, K> keyedAdjStream =  adjStream.keyBy(new KeySelector<Tuple2<K,Iterable<K>>, K>() {
					@Override
					public K getKey(Tuple2<K, Iterable<K>> tpl) throws Exception {
						return tpl.f0;
					}
				});
		return keyedAdjStream
				.timeWindow(granularity)
				.iterateSyncFor(numIterations,
						new SnapshotLoopWrapper<>(iterativeComputation),
						new SnapshotFeedbackBuilder(),
						iterativeComputation.getMessageTypeInfo());

	}
	
	private static final class EdgeFoldWrapper<K, EV> implements EdgesApply<K, EV, Tuple2<K, Iterable<K>>>, ResultTypeQueryable<Tuple2<K, Iterable<K>>>{

		private final TypeInformation<K> keyType;

		public EdgeFoldWrapper (TypeInformation<K> keyType){
			this.keyType = keyType;
		}
		
		@Override
		public void applyOnEdges(K vertexID, Iterable<Tuple2<K, EV>> neighbors, Collector<Tuple2<K, Iterable<K>>> out) throws Exception {
			Set<K> neighborhood = new HashSet<>();
			for(Tuple2<K, EV> edge : neighbors){
				neighborhood.add(edge.f0);
			}
			out.collect(new Tuple2<>(vertexID, neighborhood));
		}

		@Override
		public TypeInformation<Tuple2<K, Iterable<K>>> getProducedType() {
			return new TupleTypeInfo<>(keyType, keyType); 
		}
	}
	
	private static class SnapshotLoopWrapper<K, VV, Message extends VertexMessage<K>, OUT> implements WindowLoopFunction<Tuple2<K,Iterable<K>>, Message, OUT, Message, K, TimeWindow>{

		private final GraphSnapshotIteration<K, VV, Message, OUT> userIteration;

		//TODO integrate with managed state once it is supported for IterativeWindows
		private Map<List<Long>, Map<K, Tuple2<Iterable<K>,VV>>> statePerContext = new HashMap<>(); 
		
		public SnapshotLoopWrapper(GraphSnapshotIteration<K, VV, Message, OUT> userConfiguration){
			this.userIteration = userConfiguration;
		}

		@Override
		public void entry(K vertexID, TimeWindow timeWindow, Iterable<Tuple2<K, Iterable<K>>> contents, Collector<Either<Message, OUT>> out) throws Exception {
			Map<K, Tuple2<Iterable<K>, VV>> contextState = statePerContext.get(timeWindow.getTimeContext());
			if(contextState == null){
				contextState = new HashMap<>();
				statePerContext.put(timeWindow.getTimeContext(), contextState);
			}

			Tuple2<K, Iterable<K>> adjList = contents.iterator().next();
			contextState.put(vertexID, new Tuple2<>(adjList.f1, userIteration.initialState()));
			userIteration.preCompute(new VertexContext<>(vertexID, contextState.get(vertexID), out));
		}

		@Override
		public void step(K vertexID, TimeWindow timeWindow, Iterable<Message> messages, Collector<Either<Message, OUT>> collector) throws Exception {
			userIteration.compute(new VertexContext<>(vertexID, statePerContext.get(timeWindow.getTimeContext()).get(vertexID), collector), messages);
		}

		@Override
		public void onTermination(List<Long> list, Collector<Either<Message, OUT>> collector) throws Exception {
			Map<K, Tuple2<Iterable<K>, VV>> graphDone = statePerContext.get(list);

			for (Map.Entry<K, Tuple2<Iterable<K>, VV>> entry: graphDone.entrySet()){
				userIteration.postCompute(new VertexContext<>(entry.getKey(), entry.getValue(), collector), new Collector<OUT>() {
					@Override
					public void collect(OUT out) {
						collector.collect(Either.Right(out));
					}
					@Override
					public void close() {
						collector.close();
					}
				});
			}
			
			//we are done here with this context
			statePerContext.remove(list);
		}
	}
	
	private static class SnapshotFeedbackBuilder<Message extends VertexMessage<K>, K> implements FeedbackBuilder<Message, K> {
		
		@Override
		public KeyedStream<Message, K> feedback(DataStream<Message> dataStream) {
			return dataStream.keyBy(new KeySelector<Message, K>() {
				@Override
				public K getKey(Message message) throws Exception {
					return message.getKey();
				}
			});
		}
	}

	/**
	 * Performs a generic neighborhood aggregation in the graph window stream.
	 * Each vertex can produce zero, one or more values from the computation on its neighborhood.
	 * 
	 * @param applyFunction the neighborhood computation function
	 * @return the result stream after applying the user-defined operation on the window
	 */
	public <T> SingleOutputStreamOperator<T> applyOnNeighbors(final EdgesApply<K, EV, T> applyFunction) {
		return rawSnapshotStream.apply(new SnapshotFunction<>(applyFunction));
	}

	@SuppressWarnings("serial")
	public static class SnapshotFunction<K, EV, T> extends WrappingFunction<EdgesApply<K, EV, T>> implements
			WindowFunction<Edge<K, EV>, T, K, TimeWindow>, ResultTypeQueryable<T> {

		public SnapshotFunction(EdgesApply<K, EV, T> applyFunction) {
			super(applyFunction);
		}

		public void apply(K key, TimeWindow window,	final Iterable<Edge<K, EV>> edges, Collector<T> out)
				throws Exception {

			final Iterator<Tuple2<K, EV>> neighborsIterator = new Iterator<Tuple2<K, EV>>() {

				final Iterator<Edge<K, EV>> edgesIterator = edges.iterator();

				@Override
				public boolean hasNext() {
					return edgesIterator.hasNext();
				}

				@Override
				public Tuple2<K, EV> next() {
					Edge<K, EV> nextEdge = edgesIterator.next();
					return new Tuple2<K, EV>(nextEdge.f1, nextEdge.f2);
				}

				@Override
				public void remove() {
					edgesIterator.remove();
				}
			};

			Iterable<Tuple2<K, EV>> neighborsIterable = new Iterable<Tuple2<K, EV>>() {
				public Iterator<Tuple2<K, EV>> iterator() {
					return neighborsIterator;
				}
			};

			getWrappedFunction().applyOnEdges(key, neighborsIterable, out);
		}

		@Override
		public TypeInformation<T> getProducedType() {
			TypeInformation<T> typeInfo = TypeExtractor.createTypeInfo(EdgesApply.class, getWrappedFunction().getClass(), 2,
					null, null);
			return typeInfo;
		}
	}
}
