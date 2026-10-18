package com.example.kafkaapplication.entity;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

@Entity
@Table(name = "delivery_attempts")
public class DeliveryAttempt {
    @EmbeddedId
    private DeliveryAttemptId id;

    private int attempts;

    protected DeliveryAttempt() {
    }

    public DeliveryAttempt(DeliveryAttemptId id, int attempts) {
        this.id = id;
        this.attempts = attempts;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }
}
