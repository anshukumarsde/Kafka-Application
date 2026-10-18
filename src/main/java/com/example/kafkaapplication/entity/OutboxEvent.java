package com.example.kafkaapplication.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 200)
    private String topicName;

    @Column(nullable = false, length = 500)
    private String messageKey;

    @Column(nullable = false, columnDefinition = "clob")
    private String valueJson;

    @Column(nullable = false)
    private boolean published;

    protected OutboxEvent() {
    }

    public OutboxEvent(String topicName, String messageKey, String valueJson) {
        this.topicName = topicName;
        this.messageKey = messageKey;
        this.valueJson = valueJson;
    }

    public Long getId() {
        return id;
    }

    public String getTopicName() {
        return topicName;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getValueJson() {
        return valueJson;
    }

    public boolean isPublished() {
        return published;
    }

    public void markPublished() {
        published = true;
    }
}
