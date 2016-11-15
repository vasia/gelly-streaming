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
        int compId = Integer.MAX_VALUE;
        for (int i : ids) {
            compId = Math.min(compId, i);
        }
    }

    public int getComponentId() {
        return compId;
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
}
