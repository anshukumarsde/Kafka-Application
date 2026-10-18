package com.example.kafkaapplication.service;

import com.example.kafkaapplication.entity.OutboxEvent;
import com.example.kafkaapplication.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxService {
    private final OutboxEventRepository outboxRepository;
    private final MessageService messageService;
    private final ObjectMapper objectMapper;

    public OutboxService(
            OutboxEventRepository outboxRepository,
            MessageService messageService,
            ObjectMapper objectMapper) {
        this.outboxRepository = outboxRepository;
        this.messageService = messageService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public int relayPending() {
        return relayPendingInTransaction();
    }

    @Scheduled(fixedDelayString = "${app.outbox.relay-delay-ms:1000}")
    @Transactional
    public void relayOnSchedule() {
        relayPendingInTransaction();
    }

    private int relayPendingInTransaction() {
        var events = outboxRepository.findTop100ByPublishedFalseOrderByIdAsc();
        for (OutboxEvent event : events) {
            try {
                messageService.publish(
                        event.getTopicName(),
                        event.getMessageKey(),
                        objectMapper.readTree(event.getValueJson()));
            } catch (JsonProcessingException exception) {
                throw new IllegalStateException(
                        "Outbox event " + event.getId() + " contains invalid JSON", exception);
            }
            event.markPublished();
        }
        return events.size();
    }
}
