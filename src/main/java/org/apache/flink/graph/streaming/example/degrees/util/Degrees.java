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

package org.apache.flink.graph.streaming.example.degrees.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class Degrees extends Tuple2<Map<Long, Long>, Boolean> {

	public Degrees() {}

	public Degrees(boolean merge) {
		this.f0 = new HashMap<>();
		this.f1 = merge;
	}

	public Degrees(Degrees input, boolean merge) throws Exception {
		this(merge);
		this.set(input.getMap());
	}

	public Map<Long, Long> getMap() {
		return this.f0;
	}

	public boolean getMerge() {
		return this.f1;
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
