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

package org.apache.flink.graph.streaming.util;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

public class SampledEdge extends Tuple5<Integer, Integer, Edge<Long, NullValue>, Integer, Boolean> {

	public SampledEdge() {}

	public SampledEdge(int subtask, int instance, Edge<Long, NullValue> edge, int edgeCount, boolean resample) throws Exception {
		this.f0 = subtask;
		this.f1 = instance;
		this.f2 = edge;
		this.f3 = edgeCount;
		this.f4 = resample;
	}

	public int getSubTask() {
		return this.f0;
	}

	public int getInstance() {
		return this.f1;
	}

	public Edge<Long, NullValue> getEdge() {
		return this.f2;
	}

	public int getEdgeCount() {
		return this.f3;
	}

	public boolean isResampled() {
		return this.f4;
	}
}
