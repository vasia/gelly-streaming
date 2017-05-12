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

package org.apache.flink.graph.streaming.library;

import org.apache.flink.graph.streaming.SummaryTreeReduce;
import org.apache.flink.graph.streaming.summaries.DisjointSet;

import java.io.Serializable;

public class ConnectedComponentsTree<K extends Serializable, EV> extends SummaryTreeReduce<K, EV, DisjointSet<K>, DisjointSet<K>> implements Serializable {

	public ConnectedComponentsTree(long mergeWindowTime, int degree) {
		super(new ConnectedComponents.UpdateCC(), new ConnectedComponents.CombineCC(), new DisjointSet<K>(), mergeWindowTime, false, degree);
	}

	public ConnectedComponentsTree(long mergeWindowTime) {
		super(new ConnectedComponents.UpdateCC(), new ConnectedComponents.CombineCC(), new DisjointSet<K>(), mergeWindowTime, false);
	}
	
}







