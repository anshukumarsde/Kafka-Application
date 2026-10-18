package com.example.kafkaapplication.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class ConsumerOffsetId implements Serializable {
    @Column(name = "group_id", length = 200)
    private String groupId;

    @Column(name = "topic_name", length = 200)
    private String topicName;

    @Column(name = "partition_id")
    private int partition;

    protected ConsumerOffsetId() {
    }

    public ConsumerOffsetId(String groupId, String topicName, int partition) {
        this.groupId = groupId;
        this.topicName = topicName;
        this.partition = partition;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof ConsumerOffsetId that)) return false;
        return partition == that.partition
                && Objects.equals(groupId, that.groupId)
                && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, topicName, partition);
    }
}
