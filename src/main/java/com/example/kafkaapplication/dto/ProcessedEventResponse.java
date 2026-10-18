package com.example.kafkaapplication.dto;

public record ProcessedEventResponse(
        String groupId,
        String topic,
        int partition,
        long offset,
        String key) {
}
