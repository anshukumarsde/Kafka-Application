package com.example.kafkaapplication.entity;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "consumer_offsets")
public class ConsumerOffset {
    @EmbeddedId
    private ConsumerOffsetId id;

    private long nextOffset;

    protected ConsumerOffset() {
    }

    public ConsumerOffset(ConsumerOffsetId id) {
        this.id = id;
        this.nextOffset = 0;
    }

    public ConsumerOffsetId getId() {
        return id;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void advanceTo(long offset) {
        if (offset > nextOffset) {
            nextOffset = offset;
        }
    }
}
