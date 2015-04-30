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

package org.apache.flink.graph.streaming;

/**
 *
 * Stores properties of the streamed graph
 *
 */
public class GraphConfiguration {

	// When set to true, degrees will be collected in a separate stream
	private boolean collectInDegrees;
	private boolean collectOutDegrees;

	public GraphConfiguration() {
		collectInDegrees = false;
		collectOutDegrees = false;
	}

	public boolean getCollectDegrees() {
		return collectInDegrees || collectOutDegrees;
	}

	public void setCollectDegrees(boolean value) {
		collectInDegrees = value;
		collectOutDegrees = value;
	}

	public boolean getCollectInDegrees() {
		return collectInDegrees;
	}

	public void setCollectInDegrees(boolean value) {
		collectInDegrees = value;
	}

	public boolean getCollectOutDegrees() {
		return collectOutDegrees;
	}

	public void setCollectOutDegrees(boolean value) {
		collectOutDegrees = value;
	}
}
