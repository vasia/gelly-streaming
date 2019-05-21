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

import java.io.Serializable;
import java.util.*;


public class DisjointSet<R extends Serializable> implements Serializable {

	private static final long serialVersionUID = 1L;
	private Map<R, R> matches;
    private Map<R, Integer> ranks;

    public DisjointSet() {
    	matches = new HashMap<>();
        ranks = new HashMap<>();
    }

    public DisjointSet(Set<R> elements) {
    	this();
        for (R element : elements) {
            matches.put(element, element);
            ranks.put(element, 0);
        }
    }

    public Map<R, R> getMatches() {
        return matches;
    }

    /**
     * Creates a new disjoined set solely with e
     *
     * @param e
     */
    public void makeSet(R e) {
        matches.put(e, e);
        ranks.put(e, 0);
    }

    /**
     * Find returns the root of the disjoint set e belongs in.
     * It implements path compression, flattening the tree whenever used, attaching nodes directly to the disjoint
     * set root if not already.
     *
     * @param e
     * @return the root of the connected component
     */
    public R find(R e) {
        if (!matches.containsKey(e)) {
            return null;
        }

        R parent = matches.get(e);
        if (!parent.equals(e)) {
            R tmp = find(parent);
            if (!parent.equals(tmp)) {
                parent = tmp;
                matches.put(e, parent);
            }
        }
        return parent;
    }

    /**
     * Union combines the two possibly disjoint sets where e1 and e2 belong in.
     * Optimizations:
     * <p/>
     * - In case e1 or e2 do not exist they are being added directly in the same disjoint set.
     * - Union by Rank to minimize lookup depth
     *
     * @param e1
     * @param e2
     */
    public void union(R e1, R e2) {

        if (!matches.containsKey(e1)) {
            makeSet(e1);
        }
        if (!matches.containsKey(e2)) {
            makeSet(e2);
        }

        R root1 = find(e1);
        R root2 = find(e2);

        if (root1.equals(root2)) {
            return;
        }

        int dist1 = ranks.get(root1);
        int dist2 = ranks.get(root2);
        if (dist1 > dist2) {
            matches.put(root2, root1);
        } else if (dist1 < dist2) {
            matches.put(root1, root2);
        } else {
            matches.put(root2, root1);
            ranks.put(root1, dist1 + 1);
        }
    }

    /**
     * Merge works in a similar fashion to a naive symmetric hash join.
     * We keep the current disjoint sets and attach all nodes of 'other' incrementally
     * There is certainly room for further optimisations...
     *
     * @param other
     */
    public void merge(DisjointSet<R> other) {
        for (Map.Entry<R, R> entry : other.getMatches().entrySet()) {
            union(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String toString() {
        Map<R, List<R>> comps = new HashMap<>();

        for (R vertex : getMatches().keySet()) {
            R parent = find(vertex);
            if (!comps.containsKey(parent)) {
                List<R> vertices = new ArrayList<>();
                vertices.add(vertex);
                comps.put(parent, vertices);
            } else {
                List<R> cc = comps.get(parent);
                cc.add(vertex);
                comps.put(parent, cc);
            }
        }
        return comps.toString();
    }
}

