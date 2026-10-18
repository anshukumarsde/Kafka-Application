package com.example.kafkaapplication.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "consumer_groups")
public class ConsumerGroup {
    @Id
    @Column(name = "group_id", length = 200)
    private String groupId;

    protected ConsumerGroup() {
    }

    public ConsumerGroup(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupId() {
        return groupId;
    }
}
