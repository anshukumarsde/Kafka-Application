package com.example.kafkaapplication.service;

import com.example.kafkaapplication.entity.EventMessage;
import com.example.kafkaapplication.entity.ProcessedEvent;
import com.example.kafkaapplication.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class DemoMessageHandler implements MessageHandler {
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public DemoMessageHandler(
            ProcessedEventRepository processedEventRepository,
            ObjectMapper objectMapper) {
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void handle(String groupId, EventMessage message) {
        try {
            JsonNode value = objectMapper.readTree(message.getValueJson());
            if (value.path("simulateFailure").asBoolean(false)) {
                throw new IllegalStateException("Simulated handler failure");
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException exception) {
            throw new IllegalStateException("Stored message contains invalid JSON", exception);
        }
        processedEventRepository.save(new ProcessedEvent(groupId, message));
    }
}
