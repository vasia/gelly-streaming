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


import org.apache.flink.api.java.tuple.Tuple2;

public class GraphMessage<K, MESSAGE> extends Tuple2<K, MESSAGE> {
	
	public GraphMessage() {}
	
	public GraphMessage(K targetID, MESSAGE msg){
		this.f0 = targetID;
		this.f1 = msg;
	}
	
	public K getKey(){
		return this.f0;
	}
	
	public MESSAGE getValue(){
		return this.f1;
	}
}
