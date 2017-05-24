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


import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;

/**
 * 
 * @param <K> Vertex ID Type
 * @param <VV> Computational State Type
 * @param <Message> Vertex Message Type 
 * @param <OUT> Output Type
 */
public abstract class GraphSnapshotIteration<K, VV, Message, OUT> implements Function, ResultTypeQueryable<OUT> {

	/**
	 * 
	 * @return the initial value for the vertex state
	 */
	public abstract VV initialState();

	/**
	 * @return Messages to initiate the first superstep round
	 */
	public abstract void preCompute(VertexContext<K, VV, Message, OUT> vertexCtx);

	/**
	 * The main logic during each superstep should be implemented here.
	 * @param vertexCtx
	 * @param inputMessages
	 * @return
	 */
	public abstract void compute(VertexContext<K, VV, Message, OUT> vertexCtx, Iterable<GraphMessage<K, Message>> inputMessages);

	/**
	 * This operation generates the output events, scoped by vertex
	 * @param vertexCtx
	 * @param out
	 * @return
	 */
	public abstract void postCompute(VertexContext<K, VV, Message, OUT> vertexCtx, Collector<OUT> out);

	/**
	 * TODO get rid of this asap
	 */
	public abstract TypeInformation<Message> getMessageType();
	
}
