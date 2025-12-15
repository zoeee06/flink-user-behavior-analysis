package com.zoeliu.flink.model;

public class UserBehavior {
    public String userId;
    public String itemId;
    public String categoryId;
    public String behavior;
    public Long timestamp;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.behavior = behavior;
        this.categoryId = categoryId;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId='" + userId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", categoryId='" + categoryId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
