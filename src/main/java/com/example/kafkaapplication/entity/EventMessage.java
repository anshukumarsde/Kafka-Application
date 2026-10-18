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
@Table(name = "event_messages",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_message_partition_offset",
                columnNames = {"topic_name", "partition_id", "message_offset"}),
        indexes = @Index(name = "idx_message_read", columnList = "topic_name, partition_id, message_offset"))
public class EventMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "topic_name", nullable = false, length = 200)
    private String topicName;

    @Column(name = "partition_id", nullable = false)
    private int partition;

    @Column(name = "message_offset", nullable = false)
    private long offset;

    @Column(name = "message_key", nullable = false, length = 500)
    private String key;

    @Column(name = "value_json", nullable = false, columnDefinition = "clob")
    private String valueJson;

    protected EventMessage() {
    }

    public EventMessage(String topicName, int partition, long offset, String key, String valueJson) {
        this.topicName = topicName;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.valueJson = valueJson;
    }

    public Long getId() {
        return id;
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

    public String getValueJson() {
        return valueJson;
    }
}
