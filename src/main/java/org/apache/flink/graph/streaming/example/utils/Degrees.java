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

import java.util.HashMap;
import java.util.Map;

public class Degrees extends Tuple3<Integer, Map<Long, Long>, Boolean> {

	public Degrees() {}

	public Degrees(int source, boolean merge) {
		this.f0 = source;
		this.f1 = new HashMap<>();
		this.f2 = merge;
	}

	public Degrees(int source, Map<Long, Long> map, boolean merge) {
		this.f0 = source;
		this.f1 = map;
		this.f2 = merge;
	}

	public Degrees(int source, Degrees input, boolean merge) throws Exception {
		this(source, merge);
		this.set(input.getMap());
	}

	public int getSource() {
		return this.f0;
	}

	public Map<Long, Long> getMap() {
		return this.f1;
	}

	public boolean getMerge() {
		return this.f2;
	}

	public void set(Map<Long, Long> degrees) {
		for (Map.Entry<Long, Long> entry : degrees.entrySet()) {
			this.set(entry.getKey(), entry.getValue());
		}
	}

	public void set(long vertex, long degree) {
		this.getMap().put(vertex, degree);
	}

	public void add(Map<Long, Long> degrees) {
		for (Map.Entry<Long, Long> entry : degrees.entrySet()) {
			if (!this.getMap().containsKey(entry.getKey())) {
				this.getMap().put(entry.getKey(), 0L);
			}
			this.getMap().put(entry.getKey(), this.getMap().get(entry.getKey()) + entry.getValue());
		}
	}
}
