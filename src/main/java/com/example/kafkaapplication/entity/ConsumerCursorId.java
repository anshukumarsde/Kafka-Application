package com.example.kafkaapplication.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class ConsumerCursorId implements Serializable {
    @Column(name = "group_id", length = 200)
    private String groupId;

    @Column(name = "topic_name", length = 200)
    private String topicName;

    protected ConsumerCursorId() {
    }

    public ConsumerCursorId(String groupId, String topicName) {
        this.groupId = groupId;
        this.topicName = topicName;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ConsumerCursorId that)) return false;
        return Objects.equals(groupId, that.groupId)
                && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicName);
    }
}
