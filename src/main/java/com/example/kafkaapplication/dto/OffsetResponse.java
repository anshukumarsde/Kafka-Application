package com.example.kafkaapplication.dto;

public record OffsetResponse(String groupId, String topic, int partition, long nextOffset) {
}
