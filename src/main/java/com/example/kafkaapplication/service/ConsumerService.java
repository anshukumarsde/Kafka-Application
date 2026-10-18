package com.example.kafkaapplication.service;

import com.example.kafkaapplication.dto.ConsumeResultResponse;
import com.example.kafkaapplication.dto.OffsetResponse;
import com.example.kafkaapplication.entity.ConsumerCursor;
import com.example.kafkaapplication.entity.ConsumerCursorId;
import com.example.kafkaapplication.entity.ConsumerOffset;
import com.example.kafkaapplication.entity.ConsumerOffsetId;
import com.example.kafkaapplication.entity.DeliveryAttempt;
import com.example.kafkaapplication.entity.DeliveryAttemptId;
import com.example.kafkaapplication.entity.EventMessage;
import com.example.kafkaapplication.entity.Topic;
import com.example.kafkaapplication.exception.NotFoundException;
import com.example.kafkaapplication.repository.ConsumerCursorRepository;
import com.example.kafkaapplication.repository.ConsumerGroupRepository;
import com.example.kafkaapplication.repository.ConsumerOffsetRepository;
import com.example.kafkaapplication.repository.DeliveryAttemptRepository;
import com.example.kafkaapplication.repository.EventMessageRepository;
import com.example.kafkaapplication.repository.TopicRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Service
public class ConsumerService {
    private final ConsumerGroupRepository groupRepository;
    private final TopicRepository topicRepository;
    private final ConsumerOffsetRepository offsetRepository;
    private final ConsumerCursorRepository cursorRepository;
    private final DeliveryAttemptRepository attemptRepository;
    private final EventMessageRepository messageRepository;
    private final MessageService messageService;
    private final MessageHandler messageHandler;
    private final ObjectMapper objectMapper;

    public ConsumerService(
            ConsumerGroupRepository groupRepository,
            TopicRepository topicRepository,
            ConsumerOffsetRepository offsetRepository,
            ConsumerCursorRepository cursorRepository,
            DeliveryAttemptRepository attemptRepository,
            EventMessageRepository messageRepository,
            MessageService messageService,
            MessageHandler messageHandler,
            ObjectMapper objectMapper) {
        this.groupRepository = groupRepository;
        this.topicRepository = topicRepository;
        this.offsetRepository = offsetRepository;
        this.cursorRepository = cursorRepository;
        this.attemptRepository = attemptRepository;
        this.messageRepository = messageRepository;
        this.messageService = messageService;
        this.messageHandler = messageHandler;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public ConsumeResultResponse consumeOne(String groupId, String topicName, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        requireGroup(groupId);
        Topic topic = topicRepository.findByNameForUpdate(topicName)
                .orElseThrow(() -> new NotFoundException("Topic not found: " + topicName));
        List<ConsumerOffset> offsets = ensureOffsets(groupId, topic);
        ConsumerCursorId cursorId = new ConsumerCursorId(groupId, topicName);
        ConsumerCursor cursor = cursorRepository.findById(cursorId)
                .orElse(new ConsumerCursor(cursorId, topic.getPartitionCount() - 1));

        List<ConsumerOffset> scanOrder = new ArrayList<>();
        for (int step = 1; step <= topic.getPartitionCount(); step++) {
            int partition = (cursor.getLastPartition() + step) % topic.getPartitionCount();
            offsets.stream()
                    .filter(candidate -> candidate.getId().getPartition() == partition)
                    .findFirst()
                    .ifPresent(scanOrder::add);
        }

        for (ConsumerOffset offset : scanOrder) {
            int partition = offset.getId().getPartition();
            EventMessage message = messageRepository
                    .findFirstByTopicNameAndPartitionAndOffset(
                            topicName, partition, offset.getNextOffset())
                    .orElse(null);
            if (message == null) {
                continue;
            }
            cursor.setLastPartition(partition);
            cursorRepository.save(cursor);
            return process(groupId, topic, offset, message, maxAttempts);
        }
        return new ConsumeResultResponse("empty", null, 0, null);
    }

    @Transactional
    public List<OffsetResponse> offsets(String groupId, String topicName) {
        requireGroup(groupId);
        Topic topic = topicRepository.findById(topicName)
                .orElseThrow(() -> new NotFoundException("Topic not found: " + topicName));
        return ensureOffsets(groupId, topic).stream()
                .map(offset -> new OffsetResponse(
                        groupId,
                        topicName,
                        offset.getId().getPartition(),
                        offset.getNextOffset()))
                .toList();
    }

    private ConsumeResultResponse process(
            String groupId,
            Topic topic,
            ConsumerOffset offset,
            EventMessage message,
            int maxAttempts) {
        DeliveryAttemptId attemptId = new DeliveryAttemptId(
                groupId, topic.getName(), message.getPartition(), message.getOffset());
        DeliveryAttempt attempt = attemptRepository.findById(attemptId)
                .orElse(new DeliveryAttempt(attemptId, 0));
        int currentAttempt = attempt.getAttempts() + 1;
        try {
            messageHandler.handle(groupId, message);
        } catch (RuntimeException exception) {
            attempt.setAttempts(currentAttempt);
            attemptRepository.save(attempt);
            if (currentAttempt >= maxAttempts) {
                publishToDeadLetter(topic, message, exception);
                offset.advanceTo(message.getOffset() + 1);
                offsetRepository.save(offset);
                attemptRepository.deleteById(attemptId);
                return new ConsumeResultResponse(
                        "dead_lettered",
                        messageService.toResponse(message),
                        currentAttempt,
                        exception.getMessage());
            }
            return new ConsumeResultResponse(
                    "retry",
                    messageService.toResponse(message),
                    currentAttempt,
                    exception.getMessage());
        }

        offset.advanceTo(message.getOffset() + 1);
        offsetRepository.save(offset);
        attemptRepository.deleteById(attemptId);
        return new ConsumeResultResponse(
                "processed", messageService.toResponse(message), currentAttempt, null);
    }

    private void publishToDeadLetter(Topic sourceTopic, EventMessage message, RuntimeException error) {
        String deadLetterTopic = sourceTopic.getName() + ".DLQ";
        topicRepository.findByNameForUpdate(deadLetterTopic).orElseGet(
                () -> topicRepository.save(new Topic(deadLetterTopic, sourceTopic.getPartitionCount())));
        var value = objectMapper.createObjectNode();
        value.put("sourceTopic", message.getTopicName());
        value.put("sourcePartition", message.getPartition());
        value.put("sourceOffset", message.getOffset());
        value.put("error", error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage());
        try {
            value.set("value", objectMapper.readTree(message.getValueJson()));
        } catch (com.fasterxml.jackson.core.JsonProcessingException exception) {
            throw new IllegalStateException("Stored message contains invalid JSON", exception);
        }
        messageService.publish(deadLetterTopic, message.getKey(), value);
    }

    private List<ConsumerOffset> ensureOffsets(String groupId, Topic topic) {
        List<ConsumerOffset> offsets = new ArrayList<>();
        for (int partition = 0; partition < topic.getPartitionCount(); partition++) {
            ConsumerOffsetId id = new ConsumerOffsetId(groupId, topic.getName(), partition);
            offsets.add(offsetRepository.findById(id)
                    .orElseGet(() -> offsetRepository.save(new ConsumerOffset(id))));
        }
        return offsets;
    }

    private void requireGroup(String groupId) {
        if (!groupRepository.existsById(groupId)) {
            throw new NotFoundException("Consumer group not found: " + groupId);
        }
    }
}
