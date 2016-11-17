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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.graph.streaming.EdgesFold;
import org.apache.flink.graph.streaming.WindowGraphAggregation;
import org.apache.flink.graph.streaming.util.BitMapDisjointSet;
import org.apache.flink.types.NullValue;

import java.io.Serializable;

/**
 * Weakly connected components using RoaringBitMap as disjoint set summary
 */
public class BitMapConnectedComponents extends WindowGraphAggregation<Integer, NullValue, BitMapDisjointSet, BitMapDisjointSet> implements Serializable {

	public BitMapConnectedComponents(long mergeWindowTime) {
		super(new UpdateCC(), new CombineCC(), new BitMapDisjointSet(), mergeWindowTime, false);
	}

	public final static class UpdateCC implements EdgesFold<Integer, NullValue, BitMapDisjointSet> {

		@Override
		public BitMapDisjointSet foldEdges(BitMapDisjointSet ds, Integer src, Integer trg, NullValue edgeValue) throws Exception {
			ds.mergeEdge(src, trg);
			return ds;
		}
	}

	public static class CombineCC implements ReduceFunction<BitMapDisjointSet> {

		@Override
		public BitMapDisjointSet reduce(BitMapDisjointSet s1, BitMapDisjointSet s2) throws Exception {
			int count1 = s1.getRoaringBitmapSet().size();
			int count2 = s2.getRoaringBitmapSet().size();
			if (count1 <= count2) {
				s2.merge(s1);
				return s2;
			}
			s1.merge(s2);
			return s1;
		}
	}
}







