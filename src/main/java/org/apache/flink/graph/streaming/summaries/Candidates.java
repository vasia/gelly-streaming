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

package org.apache.flink.graph.streaming.summaries;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.streaming.util.SignedVertex;

import java.io.Serializable;
import java.util.*;

public class Candidates extends Tuple2<Boolean, TreeMap<Long, Map<Long, SignedVertex>>> implements Serializable{

	public Candidates() {}

	public Candidates(boolean success) {
		this.f0 = success;
		this.f1 = new TreeMap<>();
	}

	public Candidates(boolean success, Candidates input) throws Exception {
		this(success);

		for (Map.Entry<Long, Map<Long, SignedVertex>> entry : input.getMap().entrySet()) {
			this.add(entry.getKey(), entry.getValue());
		}
	}

	public boolean getSuccess() {
		return this.f0;
	}

	public TreeMap<Long, Map<Long, SignedVertex>> getMap() {
		return this.f1;
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
	
	//TODO clean up
	public Candidates merge(Candidates input) throws Exception {
		// Propagate failure
		if (!input.getSuccess() || !this.getSuccess()) {
			return fail();
		}

		// Compare each input component with each candidate component and merge accordingly
		for (Map.Entry<Long, Map<Long, SignedVertex>> inEntry : input.getMap().entrySet()) {

			List<Long> mergeWith = new ArrayList<>();

			for (Map.Entry<Long, Map<Long, SignedVertex>> selfEntry : this.getMap().entrySet()) {
				long selfKey = selfEntry.getKey();

				// If the two components are exactly the same, skip them
				if (inEntry.getValue().keySet().containsAll(selfEntry.getValue().keySet())
						&& selfEntry.getValue().keySet().containsAll(inEntry.getValue().keySet())) {
					continue;
				}

				// Find vertices of input component in the candidate component
				for (long inVertex : inEntry.getValue().keySet()) {
					if (selfEntry.getValue().containsKey(inVertex)) {
						if (!mergeWith.contains(selfKey)) {
							mergeWith.add(selfKey);
							break;
						}
					}
				}
			}

			if (mergeWith.isEmpty()) {
				// If the input component is disjoint from all components of the candidate,
				// simply add that component
				this.add(inEntry.getKey(), inEntry.getValue());
			} else {
				// Merge the input with the lowest id component in candidate
				Collections.sort(mergeWith);
				long firstKey = mergeWith.get(0);
				boolean success;

				success = merge(input, this,inEntry.getKey(), firstKey);
				if (!success) {
					return fail();
				}

				firstKey = Math.min(inEntry.getKey(), firstKey);

				// Merge other components of candidate into the lowest id component
				for (int i = 1; i < mergeWith.size(); ++i) {

					success = merge(this, this, mergeWith.get(i), firstKey);
					if (!success) {
						fail();
					}

					this.getMap().remove(mergeWith.get(i));
				}
			}
		}

		return this;
	}


	private boolean merge(Candidates input, Candidates candidates, long inputKey, long selfKey) throws Exception {
		Map<Long, SignedVertex> inputComponent = input.getMap().get(inputKey);
		Map<Long, SignedVertex> selfComponent = candidates.getMap().get(selfKey);

		// Find the vertices to merge along
		List<Long> mergeBy = new ArrayList<>();

		for (long inputVertex : inputComponent.keySet()) {
			if (selfComponent.containsKey(inputVertex)) {
				mergeBy.add(inputVertex);
			}
		}

		// Determine if the merge should be with reversed signs or not
		boolean inputSign = inputComponent.get(mergeBy.get(0)).getSign();
		boolean selfSign = selfComponent.get(mergeBy.get(0)).getSign();
		boolean reversed = inputSign != selfSign;

		// Evaluate the merge
		boolean success = true;
		for (long mergeVertex : mergeBy) {
			inputSign = inputComponent.get(mergeVertex).getSign();
			selfSign = selfComponent.get(mergeVertex).getSign();
			if (reversed) {
				success = inputSign != selfSign;
			} else {
				success = inputSign == selfSign;
			}
			if (!success) {
				return false;
			}
		}

		// Execute the merge
		long commonKey = Math.min(inputKey, selfKey);

		// Merge input vertices
		for (SignedVertex inputVertex : inputComponent.values()) {

			if (reversed) {
				success = candidates.add(commonKey, inputVertex.reverse());
			} else {
				success = candidates.add(commonKey, inputVertex);
			}
			if (!success) {
				return false;
			}
		}

		return true;
	}

	private Candidates fail() {
		return new Candidates(false);
	}
}
