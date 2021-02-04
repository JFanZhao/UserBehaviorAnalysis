package com.ivan.analysis.bean;

import lombok.Data;

@Data
/**
 * @author: ivan
 * @date:2021年02月04日10:47:25
 */
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Long categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior(Long userId, Long itemId, Long categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
