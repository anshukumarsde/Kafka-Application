package com.example.kafkaapplication.service;

import com.example.kafkaapplication.dto.CreateTopicRequest;
import com.example.kafkaapplication.dto.TopicResponse;
import com.example.kafkaapplication.entity.Topic;
import com.example.kafkaapplication.exception.ConflictException;
import com.example.kafkaapplication.repository.TopicRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class TopicService {
    private final TopicRepository topicRepository;

    public TopicService(TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    @Transactional
    public TopicResponse createTopic(CreateTopicRequest request) {
        Topic existing = topicRepository.findById(request.name()).orElse(null);
        if (existing != null) {
            if (existing.getPartitionCount() != request.partitions()) {
                throw new ConflictException(
                        "Topic already exists with " + existing.getPartitionCount() + " partitions");
            }
            return toResponse(existing);
        }
        return toResponse(topicRepository.save(new Topic(request.name(), request.partitions())));
    }

    @Transactional
    public void ensureTopic(String name, int partitions) {
        Topic existing = topicRepository.findById(name).orElse(null);
        if (existing == null) {
            topicRepository.save(new Topic(name, partitions));
        } else if (existing.getPartitionCount() != partitions) {
            throw new ConflictException(
                    "Topic " + name + " exists with " + existing.getPartitionCount() + " partitions");
        }
    }

    @Transactional(readOnly = true)
    public List<TopicResponse> listTopics() {
        return topicRepository.findAll().stream().map(TopicService::toResponse).toList();
    }

    @Transactional(readOnly = true)
    public Topic requireTopic(String name) {
        return topicRepository.findById(name)
                .orElseThrow(() -> new com.example.kafkaapplication.exception.NotFoundException(
                        "Topic not found: " + name));
    }

    private static TopicResponse toResponse(Topic topic) {
        return new TopicResponse(topic.getName(), topic.getPartitionCount());
    }
}
