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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;

/**
 * 
 * @param <K> vertexID type
 * @param <VV> vertex value type
 * @param <Message> vertex message type
 * @param <OUT> output result type
 */
public class VertexContext<K,VV, Message, OUT>{

	private final K id;
	private final long superstep;
	private final Tuple2<Iterable<K>, VV> vertexCtx;
	private final Collector<Either<GraphMessage<K, Message>, OUT>> wrappedOut;

	public VertexContext(K id, long superstep, Tuple2<Iterable<K>, VV> ctx, Collector<Either<GraphMessage<K, Message>, OUT>> out){
		this.id = id;
		this.superstep = superstep;
		this.vertexCtx = ctx;
		this.wrappedOut = out;
	}

	public K getVertexID(){
		return id;
	}

	public Iterable<K> getNeighbors(){
		return vertexCtx.f0;
	}

	public long getSuperstep() {
		return superstep;
	}

	public VV getVertexState(){
		return vertexCtx.f1;
	}

	public void setVertexState(VV newState){
		vertexCtx.setField(newState,1);
	}

	public void sendMessage(K targetVertex, Message msg){
		wrappedOut.collect(Either.Left(new GraphMessage<>(targetVertex, msg)));
	}
}
