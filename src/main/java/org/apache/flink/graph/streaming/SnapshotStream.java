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

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.windowing.LoopContext;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowLoopFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.progress.FixpointIterationTermination;
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
	 * Note: For completeness it is recommended to iterateFor numIterations on sliced graph streams with ALL direction.
	 *
	 * 
	 * @param iterativeComputation
	 * @param <VV> The type of computational state in focus scoped per vertex (e.g., ranks, connected components etc.)
	 * @param <Message> The type of messages between vertices circulating within the iterative computation   
	 * @param <OUT> The type of the output stream triggered by the iterative computation
	 * @return
	 */
	public <VV, Message, OUT> DataStream<OUT> iterateFor(GraphSnapshotIteration<K, VV, Message, OUT> iterativeComputation, int numIterations) throws Exception {

		WindowedStream<Tuple2<K, Iterable<K>>, K, TimeWindow> snapshotStream = makeSnapshotStream();
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) rawSnapshotStream.getInputType()).getTypeAt(0);
		TypeInformation<?> feedbackType = new TupleTypeInfo<>(GraphMessage.class, keyType, iterativeComputation.getMessageType());
		
		return snapshotStream
				.iterateSyncFor(numIterations,
						new SnapshotLoopWrapper<>(iterativeComputation),
						new SnapshotFeedbackBuilder(),
						(TypeInformation<GraphMessage<K,Message>>) feedbackType);
	}

	/**
	 * Similar to iterateFor but with Fixpoint termination
	 *
	 */
	public <VV, Message, OUT> DataStream<OUT> iterateFixpoint(GraphSnapshotIteration<K, VV, Message, OUT> iterativeComputation) throws Exception {
		
		WindowedStream<Tuple2<K, Iterable<K>>, K, TimeWindow> snapshotedStream = makeSnapshotStream();
		TypeInformation<K> keyType = ((TupleTypeInfo<?>) rawSnapshotStream.getInputType()).getTypeAt(0);
		TypeInformation<?> feedbackType = new TupleTypeInfo<>(GraphMessage.class, keyType, iterativeComputation.getMessageType());

		return snapshotedStream
				.iterateSync(
						new SnapshotLoopWrapper<>(iterativeComputation),
						new FixpointIterationTermination(),
						new SnapshotFeedbackBuilder(),
						(TypeInformation<GraphMessage<K,Message>>) feedbackType);
	
	}

	private WindowedStream<Tuple2<K, Iterable<K>>, K, TimeWindow> makeSnapshotStream() {
		//form adjacency lists to trigger iterative computation on that state

		SingleOutputStreamOperator<Tuple2<K, Iterable<K>>> adjStream =
				this.rawSnapshotStream.apply(new WindowFunction<Edge<K,EV>, Tuple2<K, Iterable<K>>, K, TimeWindow>() {
					@Override
					public void apply(K k, TimeWindow timeWindow, Iterable<Edge<K, EV>> edges, Collector<Tuple2<K, Iterable<K>>> collector) throws Exception {
						Set<K> neighborhood = new HashSet<>();
						for(Edge<K, EV> edge : edges){
							neighborhood.add(edge.f1);
						}
						Tuple2<K, Iterable<K>> next = new Tuple2<>(k, neighborhood);
						collector.collect(next);
					}
				});

		KeyedStream<Tuple2<K, Iterable<K>>, K> keyedAdjStream =  adjStream.keyBy(new KeySelector<Tuple2<K,Iterable<K>>, K>() {
			@Override
			public K getKey(Tuple2<K, Iterable<K>> tpl) throws Exception {
				return tpl.f0;
			}
		});
		return keyedAdjStream.timeWindow(granularity);
	}


	private static class SnapshotLoopWrapper<K, VV, Message, OUT> implements WindowLoopFunction<Tuple2<K,Iterable<K>>, GraphMessage<K, Message>, OUT, GraphMessage<K, Message>, K, TimeWindow>, ResultTypeQueryable<OUT>{

		private final GraphSnapshotIteration<K, VV, Message, OUT> userIteration;

		//TODO integrate with managed state once it is supported for IterativeWindows
		private Map<List<Long>, Map<K, Tuple2<Iterable<K>,VV>>> statePerContext; 
		
		public SnapshotLoopWrapper(GraphSnapshotIteration<K, VV, Message, OUT> userConfiguration){
			this.userIteration = userConfiguration;
			statePerContext = new HashedMap();
		}

		@Override
		public void entry(LoopContext<K> ctx, Iterable<Tuple2<K, Iterable<K>>> contents, Collector<Either<GraphMessage<K, Message>, OUT>> out) throws Exception {

//			System.err.println("DEBUG - ENTRY INVOKED");
			
			if(statePerContext == null){
				 statePerContext = new HashMap<>();
			}
			Map<K, Tuple2<Iterable<K>, VV>> contextState = statePerContext.get(ctx.getContext());

			if(contextState == null){
				contextState = new HashMap<>();
				statePerContext.put(ctx.getContext(), contextState);
			}

			Tuple2<K, Iterable<K>> adjList = contents.iterator().next();
			contextState.put(ctx.getKey(), new Tuple2<>(adjList.f1, userIteration.initialState()));
			userIteration.preCompute(new VertexContext<>(ctx.getKey(), 0, contextState.get(ctx.getKey()), out));
		}

		@Override
		public void step(LoopContext<K> ctx, Iterable<GraphMessage<K, Message>> messages, Collector<Either<GraphMessage<K, Message>, OUT>> collector) throws Exception {
//			System.err.println("DEBUG - STEP INVOKED : "+ctx);
			if(statePerContext.containsKey(ctx.getContext())){
				userIteration.compute(new VertexContext<>(ctx.getKey(), ctx.getSuperstep(), statePerContext.get(ctx.getContext()).get(ctx.getKey()), collector), messages);
			}
		}

		@Override
		public void onTermination(List<Long> list, long superstep, Collector<Either<GraphMessage<K, Message>, OUT>> collector) throws Exception {
//			System.err.println("DEBUG - ONTERMINATION INVOKED for context : "+list);
			if(statePerContext.containsKey(list)){
				Map<K, Tuple2<Iterable<K>, VV>> graphDone = statePerContext.get(list);

				for (Map.Entry<K, Tuple2<Iterable<K>, VV>> entry: graphDone.entrySet()){
					userIteration.postCompute(new VertexContext<>(entry.getKey(), superstep , entry.getValue(), collector), new Collector<OUT>() {
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

		@Override
		public TypeInformation<OUT> getProducedType() {
			return userIteration.getProducedType();
		}
	}
	
	private static class SnapshotFeedbackBuilder<Message, K> implements FeedbackBuilder<GraphMessage<K, Message>, K> {
		
		@Override
		public KeyedStream<GraphMessage<K, Message>, K> feedback(DataStream<GraphMessage<K, Message>> dataStream) {
			return dataStream.keyBy(new SnapshotKeySelector<>()) ;
		}
	}
	
	public static final class SnapshotKeySelector<Message, K> implements KeySelector<GraphMessage<K, Message>, K> {

		@Override
		public K getKey(GraphMessage<K, Message> message) throws Exception {
			return message.f0;
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
