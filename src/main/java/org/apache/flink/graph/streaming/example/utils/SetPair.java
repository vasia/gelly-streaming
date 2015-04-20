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

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;
import java.util.Set;

public final class SetPair extends Tuple2<Set<Long>, Set<Long>> {

	public SetPair() {
		super();
	}

	public SetPair(Set<Long> pos, Set<Long> neg) {
		this.f0 = pos;
		this.f1 = neg;
	}

	public Set<Long> getPos() {
		return this.f0;
	}

	public Set<Long> getNeg() {
		return this.f1;
	}

	@Override
	public SetPair copy() {
		Set<Long> pos = new HashSet<>();
		pos.addAll(this.f0);

		Set<Long> neg = new HashSet<>();
		neg.addAll(this.f1);

		return new SetPair(pos, neg);
	}
}
