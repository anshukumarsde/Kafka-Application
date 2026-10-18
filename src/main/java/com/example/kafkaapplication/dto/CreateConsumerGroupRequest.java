package com.example.kafkaapplication.dto;

import jakarta.validation.constraints.NotBlank;

public record CreateConsumerGroupRequest(@NotBlank String groupId) {
}
