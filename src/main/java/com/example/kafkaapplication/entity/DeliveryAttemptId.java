package com.example.kafkaapplication.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class DeliveryAttemptId implements Serializable {
    @Column(name = "group_id", length = 200)
    private String groupId;

    @Column(name = "topic_name", length = 200)
    private String topicName;

    @Column(name = "partition_id")
    private int partition;

    @Column(name = "message_offset")
    private long offset;

    protected DeliveryAttemptId() {
    }

    public DeliveryAttemptId(String groupId, String topicName, int partition, long offset) {
        this.groupId = groupId;
        this.topicName = topicName;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DeliveryAttemptId that)) return false;
        return partition == that.partition && offset == that.offset
                && Objects.equals(groupId, that.groupId)
                && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicName, partition, offset);
    }
}
