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

package org.apache.flink.graph.streaming.example.utils;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

public class TriangleEstimate extends Tuple4<Integer, Integer, Integer, Integer> {

	public TriangleEstimate() {}

	public TriangleEstimate(int source, int edges, int vertices, int beta) throws Exception {
		this.f0 = source;
		this.f1 = edges;
		this.f2 = vertices;
		this.f3 = beta;
	}

	public int getSource() {
		return this.f0;
	}

	public int getEdgeCount() {
		return this.f1;
	}

	public int getVertexCount() {
		return this.f2;
	}

	public int getBeta() {
		return this.f3;
	}
}
