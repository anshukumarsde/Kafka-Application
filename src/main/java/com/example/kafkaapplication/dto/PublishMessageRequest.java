package com.example.kafkaapplication.dto;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record PublishMessageRequest(
        @NotBlank String key,
        @NotNull JsonNode value) {
}
