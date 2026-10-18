package com.example.kafkaapplication.controller;

import com.example.kafkaapplication.service.OutboxService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/outbox")
public class OutboxController {
    private final OutboxService outboxService;

    public OutboxController(OutboxService outboxService) {
        this.outboxService = outboxService;
    }

    @PostMapping("/relay")
    public Map<String, Integer> relay() {
        return Map.of("published", outboxService.relayPending());
    }
}
