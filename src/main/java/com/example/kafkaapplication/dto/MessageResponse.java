package com.example.kafkaapplication.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record MessageResponse(
        String topic,
        int partition,
        long offset,
        String key,
        JsonNode value) {
}
