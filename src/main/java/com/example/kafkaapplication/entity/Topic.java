package com.example.kafkaapplication.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "event_topics")
public class Topic {
    @Id
    @Column(length = 200)
    private String name;

    @Column(name = "partition_count", nullable = false)
    private int partitionCount;

    protected Topic() {
    }

    public Topic(String name, int partitionCount) {
        this.name = name;
        this.partitionCount = partitionCount;
    }

    public String getName() {
        return name;
    }

    public int getPartitionCount() {
        return partitionCount;
    }
}
