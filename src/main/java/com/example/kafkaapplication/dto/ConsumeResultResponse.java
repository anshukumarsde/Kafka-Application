package com.example.kafkaapplication.dto;

public record ConsumeResultResponse(
        String status,
        MessageResponse message,
        int attempts,
        String error) {
}
