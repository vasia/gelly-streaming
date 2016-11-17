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

import java.io.Serializable;
import java.util.*;

/**
 * Implements a Disjoint Set data structure using RoaringBitMaps.
 */
public class BitMapDisjointSet implements Serializable {

    private Set<RoaringBitMapComponent> roaringBitmapSet;

    public Set<RoaringBitMapComponent> getRoaringBitmapSet() {
        return roaringBitmapSet;
    }

    public BitMapDisjointSet() {
        roaringBitmapSet = new HashSet<>();
    }

    /**
     * Merge other disjoint set into this disjoint set.
     * @param other
     */
    public void merge(BitMapDisjointSet other) {
        for (RoaringBitMapComponent otherComp : other.getRoaringBitmapSet()) {
            mergeComponent(otherComp);
        }
    }

    /**
     * Merge a component into this disjoint set.
     * If the component intersects with more than one existing component,
     * these components will also be merged with each other.
     * @param other the component to merge
     */
    public void mergeComponent(RoaringBitMapComponent other) {

        List<RoaringBitMapComponent> toBeMerged = new ArrayList<>();

        for (RoaringBitMapComponent comp : roaringBitmapSet) {
            if (comp.merge(other)) {
                toBeMerged.add(comp);
            }
        }
        if (toBeMerged.size() == 0) {
            //create new component
            roaringBitmapSet.add(other);
        }
        else if (toBeMerged.size() > 1) {
            RoaringBitMapComponent first = toBeMerged.get(0);
            roaringBitmapSet.remove(first);
            for (int i = 1; i < toBeMerged.size(); i++) {
                first.merge(toBeMerged.get(i));
                roaringBitmapSet.remove(toBeMerged.get(i));
            }
            roaringBitmapSet.add(first);
        }
    }

    /**
     * Merge the edge (src, trg) into this set.
     * If the edge doesn't belong to any existing component
     * we create a new one, otherwise, we merge components accordingly.
     * @param src the edge src id
     * @param trg the edge trg id
     */
    public void mergeEdge(int src, int trg) throws Exception {

        List<RoaringBitMapComponent> toBeMerged = new ArrayList<>();
        final Iterator<RoaringBitMapComponent> compIterator = roaringBitmapSet.iterator();

        while (compIterator.hasNext() && toBeMerged.size() < 2) {
            RoaringBitMapComponent comp = compIterator.next();
            if (comp.mergeEdge(src, trg)) {
                    toBeMerged.add(comp);
            }
        }
        if (toBeMerged.size() == 0) {
            // create a new component
            roaringBitmapSet.add(new RoaringBitMapComponent(src, trg));
        }
        else if (toBeMerged.size() == 2) {
            // merge the two components
            boolean isMerged = toBeMerged.get(0).merge(toBeMerged.get(1));
            if (!isMerged) {
                throw new Exception("Attempted to merge unmergeable components with IDs " + toBeMerged.get(0).getComponentId()
                + " and " + toBeMerged.get(1).getComponentId());
            }
            // remove merged component from the set
            this.roaringBitmapSet.remove(toBeMerged.get(1));
        }
    }

    @Override
    public String toString() {
        String toPrint = "";
        for (RoaringBitMapComponent r : roaringBitmapSet) {
            toPrint += r.getComponentId() + ", ";
        }
        return toPrint;
    }
}
