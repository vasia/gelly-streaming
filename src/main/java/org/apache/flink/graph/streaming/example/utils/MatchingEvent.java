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

import java.util.HashSet;
import java.util.Set;

public class MatchingEvent extends Tuple4<Long, MatchingEvent.Type, Edge<Long, Long>, Set<Edge<Long, Long>>> {

	public enum Type {INIT, ADD, REMOVE, REPLACE, UNLOCK}

	public MatchingEvent() {}

	public MatchingEvent(long target, MatchingEvent.Type type,
			Edge<Long, Long> edge, Set<Edge<Long, Long>> collisions) throws Exception {
		this.f0 = target;
		this.f1 = type;
		this.f2 = edge;
		this.f3 = collisions;
	}

	public MatchingEvent(long target, MatchingEvent.Type type, Edge<Long, Long> edge) throws Exception {
		this.f0 = target;
		this.f1 = type;
		this.f2 = edge;

		Set<Edge<Long, Long>> empty = new HashSet<>();
		this.f3 = empty;
	}

	public long getTarget() {
		return this.f0;
	}

	public MatchingEvent.Type getType() {
		return this.f1;
	}

	public Edge<Long, Long> getEdge() {
		return this.f2;
	}

	public Set<Edge<Long, Long>> getCollisions() {
		return this.f3;
	}

}
