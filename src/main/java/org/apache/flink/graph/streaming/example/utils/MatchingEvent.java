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

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.example.DistributedWeightedMatchingExample;

public class MatchingEvent extends Tuple5<Long, MatchingEvent.Type,
		Edge<Long, Long>, Edge<Long, Long>, Edge<Long, Long>> {

	public enum Type {ADD, REMOVE, REPLACE}

	public MatchingEvent() {}

	public MatchingEvent(boolean toMaster, MatchingEvent.Type type, Edge<Long, Long> edge,
			Edge<Long, Long> collisionA, Edge<Long, Long> collisionB) {

		this.f0 = hash(edge, toMaster);
		this.f1 = type;
		this.f2 = edge;
		this.f3 = collisionA;
		this.f4 = collisionB;
	}

	public MatchingEvent(MatchingEvent.Type type, Edge<Long, Long> edge) {
		this.f0 = hash(edge, true);
		this.f1 = type;
		this.f2 = edge;
		this.f3 = edge;
		this.f4 = edge;
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

	public Edge<Long, Long> getCollisionA() {
		return this.f3;
	}

	public Edge<Long, Long> getCollisionB() {
		return this.f4;
	}

	private long hash(Edge<Long, Long> edge, boolean master) {
		int p = DistributedWeightedMatchingExample.PARALLELISM;
		long h1 = edge.getSource().hashCode() % p;

		//long h2 = edge.getTarget().hashCode() % p;
		long h2 = (h1 + 1) % p;

		return master ? h1 : h2;
	}
}
