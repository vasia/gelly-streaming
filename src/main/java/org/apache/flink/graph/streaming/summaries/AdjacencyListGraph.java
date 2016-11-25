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

/**
 * A simple, undirected adjacency list graph representation with methods for traversals.
 * Used in the Spanner library method.
 * @param <K> the vertex id type
 */
public class AdjacencyListGraph<K extends Comparable<K>> implements Serializable {

    private Map<K, HashSet<K>> adjacencyMap;

    public AdjacencyListGraph() {
        adjacencyMap = new HashMap<>();
    }

    public Map<K, HashSet<K>> getAdjacencyMap() {
        return adjacencyMap;
    }

    /**
     * Adds the edge to the current adjacency graph
     * @param src the src id
     * @param trg the trg id
     */
    public void addEdge(K src, K trg) {
        HashSet<K> neighbors;
        // add the neighbor to the src
        if (adjacencyMap.containsKey(src)) {
            neighbors = adjacencyMap.get(src);
        }
        else {
            neighbors = new HashSet<>();
        }
        neighbors.add(trg);
        adjacencyMap.put(src, neighbors);

        // add the neighbor to the trg
        if (adjacencyMap.containsKey(trg)) {
            neighbors = adjacencyMap.get(trg);
        }
        else {
            neighbors = new HashSet<>();
        }
        neighbors.add(src);
        adjacencyMap.put(trg, neighbors);
    }

    /**
     * Performs a bounded BFS on the adjacency graph to determine
     * whether the current distance between src and trg is greater than k.
     * @param src
     * @param trg
     * @param k
     * @return true if the current distance is less than or equal to k
     * and false otherwise.
     */
    //TODO: maybe k should be a property of the adjacency graph
    public boolean boundedBFS(K src, K trg, int k) {
        if (!adjacencyMap.containsKey(src)) {
            // this is the first time we encounter this vertex
            return false;
        }
        else {
            Set<K> visited = new HashSet<>();
            Queue<Node> queue = new LinkedList<>();

            // add the src neighbors
            for (K neighbor : adjacencyMap.get(src)) {
                queue.add(new Node(neighbor, 1));
            }
            visited.add(src);

            while (!queue.isEmpty()) {
                Node current = queue.peek();
                if (current.getId().equals(trg)) {
                    // we found the trg in <= k levels
                    return true;
                }
                else {
                    queue.remove();
                    visited.add(current.getId());

                    // bound the BFS to k steps
                    if (current.getLevel() < k) {
                        for (K neighbor : adjacencyMap.get(current.getId())) {
                            if (!visited.contains(neighbor)) {
                                queue.add(new Node(neighbor, current.getLevel()+1));
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    public void reset() {
        adjacencyMap.clear();
    }

    public class Node {

        private K id;
        private int level;

        Node(K id, int level) {
            this.id = id;
            this.level = level;
        }

        public K getId() {
            return id;
        }

        int getLevel() {
            return level;
        }
    }
}
