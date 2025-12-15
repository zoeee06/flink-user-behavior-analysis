package com.zoeliu.flink.model;

public class ItemViewCount {
    public String itemId;
    public long windowEnd;
    public long count;

    public ItemViewCount() {
    }

    public ItemViewCount(String itemId, long windowEnd, long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId='" + itemId + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
