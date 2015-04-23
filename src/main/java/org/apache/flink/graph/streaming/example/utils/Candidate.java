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

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;
import java.util.TreeMap;

public class Candidate extends Tuple3<Integer, Boolean, TreeMap<Long, Map<Long, SignedVertex>>> {

	public Candidate() {}

	public Candidate(int source, boolean success) {
		this.f0 = source;
		this.f1 = success;
		this.f2 = new TreeMap<>();
	}

	public Candidate(int source, boolean success, TreeMap<Long, Map<Long, SignedVertex>> map) {
		this.f0 = source;
		this.f1 = success;
		this.f2 = map;
	}

	public Candidate(int source, boolean success, Candidate input) throws Exception {
		this(source, success);

		for (Map.Entry<Long, Map<Long, SignedVertex>> entry : input.getMap().entrySet()) {
			this.add(entry.getKey(), entry.getValue());
		}
	}

	public int getSource() {
		return this.f0;
	}

	public boolean getSuccess() {
		return this.f1;
	}

	public TreeMap<Long, Map<Long, SignedVertex>> getMap() {
		return this.f2;
	}

	public boolean add(long component, Map<Long, SignedVertex> vertices) throws Exception {
		for (SignedVertex vertex : vertices.values()) {
			if (!this.add(component, vertex)) {
				return false;
			}
		}
		return true;
	}

	public boolean add(long component, SignedVertex vertex) throws Exception {
		if (!this.getMap().containsKey(component)) {
			this.getMap().put(component, new TreeMap<Long, SignedVertex>());
		}
		if (this.getMap().get(component).containsKey(vertex.getVertex())) {
			SignedVertex storedVertex = this.getMap().get(component).get(vertex.getVertex());
			if (storedVertex.getSign() != vertex.getSign()) {
				return false;
			}
		}
		this.getMap().get(component).put(vertex.getVertex(), vertex);

		return true;
	}
}
