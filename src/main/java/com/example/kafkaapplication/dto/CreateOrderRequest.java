package com.example.kafkaapplication.dto;

import jakarta.validation.constraints.NotBlank;

public record CreateOrderRequest(@NotBlank String orderId) {
}
