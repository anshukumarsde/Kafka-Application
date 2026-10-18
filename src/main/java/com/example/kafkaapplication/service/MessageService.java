package com.example.kafkaapplication.service;

import com.example.kafkaapplication.dto.MessageResponse;
import com.example.kafkaapplication.entity.EventMessage;
import com.example.kafkaapplication.entity.Topic;
import com.example.kafkaapplication.exception.NotFoundException;
import com.example.kafkaapplication.repository.EventMessageRepository;
import com.example.kafkaapplication.repository.TopicRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class MessageService {
    private final EventMessageRepository messageRepository;
    private final TopicRepository topicRepository;
    private final ObjectMapper objectMapper;

    public MessageService(
            EventMessageRepository messageRepository,
            TopicRepository topicRepository,
            ObjectMapper objectMapper) {
        this.messageRepository = messageRepository;
        this.topicRepository = topicRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public MessageResponse publish(String topicName, String key, JsonNode value) {
        Topic topic = topicRepository.findByNameForUpdate(topicName)
                .orElseThrow(() -> new NotFoundException("Topic not found: " + topicName));
        int partition = Math.floorMod(key.hashCode(), topic.getPartitionCount());
        long offset = messageRepository.findLastOffset(topicName, partition) + 1;
        EventMessage message = new EventMessage(
                topicName, partition, offset, key, writeJson(value));
        return toResponse(messageRepository.save(message));
    }

    @Transactional(readOnly = true)
    public List<MessageResponse> listMessages(String topicName, Integer partition) {
        if (partition == null) {
            return messageRepository.findAllByTopicNameOrderByPartitionAscOffsetAsc(topicName)
                    .stream().map(this::toResponse).toList();
        }
        return messageRepository
                .findAllByTopicNameAndPartitionOrderByOffsetAsc(topicName, partition)
                .stream().map(this::toResponse).toList();
    }

    private String writeJson(JsonNode value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Message value is not valid JSON", exception);
        }
    }

    public MessageResponse toResponse(EventMessage message) {
        try {
            return new MessageResponse(
                    message.getTopicName(),
                    message.getPartition(),
                    message.getOffset(),
                    message.getKey(),
                    objectMapper.readTree(message.getValueJson()));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Stored message contains invalid JSON", exception);
        }
    }
}
