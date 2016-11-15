package org.apache.flink.graph.streaming.util;

import org.roaringbitmap.RoaringBitmap;

import java.util.Set;

/**
 * Implements a Disjoint Set data structure using RoaringBitMaps.
 */
public class BitMapDisjointSet {

    private Set<RoaringBitMapComponent> roaringBitmapSet;


    public BitMapDisjointSet() {

    }
}
