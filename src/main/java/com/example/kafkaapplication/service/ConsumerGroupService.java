package com.example.kafkaapplication.service;

import com.example.kafkaapplication.dto.ConsumerGroupResponse;
import com.example.kafkaapplication.dto.ProcessedEventResponse;
import com.example.kafkaapplication.entity.ConsumerGroup;
import com.example.kafkaapplication.exception.ConflictException;
import com.example.kafkaapplication.repository.ConsumerGroupRepository;
import com.example.kafkaapplication.repository.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class ConsumerGroupService {
    private final ConsumerGroupRepository consumerGroupRepository;
    private final ProcessedEventRepository processedEventRepository;

    public ConsumerGroupService(
            ConsumerGroupRepository consumerGroupRepository,
            ProcessedEventRepository processedEventRepository) {
        this.consumerGroupRepository = consumerGroupRepository;
        this.processedEventRepository = processedEventRepository;
    }

    @Transactional
    public ConsumerGroupResponse create(String groupId) {
        if (consumerGroupRepository.existsById(groupId)) {
            throw new ConflictException("Consumer group already exists: " + groupId);
        }
        return new ConsumerGroupResponse(
                consumerGroupRepository.save(new ConsumerGroup(groupId)).getGroupId());
    }

    @Transactional(readOnly = true)
    public List<ConsumerGroupResponse> list() {
        return consumerGroupRepository.findAll().stream()
                .map(group -> new ConsumerGroupResponse(group.getGroupId())).toList();
    }

    @Transactional(readOnly = true)
    public List<ProcessedEventResponse> processed(String groupId) {
        if (!consumerGroupRepository.existsById(groupId)) {
            throw new com.example.kafkaapplication.exception.NotFoundException(
                    "Consumer group not found: " + groupId);
        }
        return processedEventRepository.findAllByGroupIdOrderByIdAsc(groupId).stream()
                .map(event -> new ProcessedEventResponse(
                        event.getGroupId(),
                        event.getTopicName(),
                        event.getPartition(),
                        event.getOffset(),
                        event.getKey()))
                .toList();
    }
}
