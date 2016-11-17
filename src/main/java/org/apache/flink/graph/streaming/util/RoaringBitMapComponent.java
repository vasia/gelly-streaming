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

package org.apache.flink.graph.streaming.util;

import org.roaringbitmap.RoaringBitmap;

/**
 * Wraps a RoaringBitMap to add convenience methods for use as a component data structure.
 */
public class RoaringBitMapComponent {

    private RoaringBitmap bitMap;
    private int compId; // the minimum bit position set in the bitMap

    public RoaringBitMapComponent(int... ids) {
        bitMap = new RoaringBitmap();
        bitMap.add(ids);
        int id = Integer.MAX_VALUE;
        for (int i : ids) {
            id = Math.min(id, i);
        }
        compId = id;
    }

    public RoaringBitMapComponent(int compId, RoaringBitmap map) {
        this.compId = compId;
        this.bitMap = map;
    }

    public int getComponentId() {
        return compId;
    }

    public RoaringBitmap getBitMap() {
        return bitMap;
    }

    /**
     * Merges the given bitmap if the intersection is not empty
     * and mutates this.
     * @param other the bitMap to merge
     * @return true if the merge happened
     */
    public boolean merge(RoaringBitMapComponent other) {
        if (compId == other.compId || RoaringBitmap.intersects(this.bitMap, other.bitMap)) {
            compId = Math.min(compId, other.compId);
            this.bitMap.or(other.bitMap);
            return true;
        }
        return false;
    }

    /**
     * Attempt to merge edge (src, trg) into this component.
     * If one of the endpoints belongs to the component, add the other endpoint,
     * update component ID if required and return true. Otherwise, return false.
     * @param src the edge source id
     * @param trg the edge target id
     * @return true if the edge was merged in this component.
     */
    public boolean mergeEdge(int src, int trg) {
        return this.merge(new RoaringBitMapComponent(src, trg));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(RoaringBitMapComponent.class)) {
            RoaringBitMapComponent other = (RoaringBitMapComponent) obj;
            return (this.compId == other.compId && this.bitMap.equals(other.bitMap));
        }
        return false;
    }
}
