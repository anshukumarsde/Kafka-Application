package com.example.kafkaapplication.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(name = "processed_events",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_processed_group_message",
                columnNames = {"group_id", "topic_name", "partition_id", "message_offset"}),
        indexes = @Index(name = "idx_processed_group", columnList = "group_id"))
public class ProcessedEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "group_id", nullable = false, length = 200)
    private String groupId;

    @Column(name = "topic_name", nullable = false, length = 200)
    private String topicName;

    @Column(name = "partition_id", nullable = false)
    private int partition;

    @Column(name = "message_offset", nullable = false)
    private long offset;

    @Column(name = "message_key", nullable = false, length = 500)
    private String key;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String groupId, EventMessage message) {
        this.groupId = groupId;
        this.topicName = message.getTopicName();
        this.partition = message.getPartition();
        this.offset = message.getOffset();
        this.key = message.getKey();
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopicName() {
        return topicName;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getKey() {
        return key;
    }
}
