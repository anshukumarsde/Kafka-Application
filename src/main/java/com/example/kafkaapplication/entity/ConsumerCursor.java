package com.example.kafkaapplication.entity;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "consumer_cursors")
public class ConsumerCursor {
    @EmbeddedId
    private ConsumerCursorId id;

    private int lastPartition;

    protected ConsumerCursor() {
    }

    public ConsumerCursor(ConsumerCursorId id, int lastPartition) {
        this.id = id;
        this.lastPartition = lastPartition;
    }

    public int getLastPartition() {
        return lastPartition;
    }

    public void setLastPartition(int lastPartition) {
        this.lastPartition = lastPartition;
    }
}
