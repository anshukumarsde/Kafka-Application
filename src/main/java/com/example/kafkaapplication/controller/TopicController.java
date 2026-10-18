package com.example.kafkaapplication.controller;

import com.example.kafkaapplication.dto.CreateTopicRequest;
import com.example.kafkaapplication.dto.MessageResponse;
import com.example.kafkaapplication.dto.PublishMessageRequest;
import com.example.kafkaapplication.dto.TopicResponse;
import com.example.kafkaapplication.service.MessageService;
import com.example.kafkaapplication.service.TopicService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/topics")
public class TopicController {
    private final TopicService topicService;
    private final MessageService messageService;

    public TopicController(TopicService topicService, MessageService messageService) {
        this.topicService = topicService;
        this.messageService = messageService;
    }

    @PostMapping
    public TopicResponse create(@Valid @RequestBody CreateTopicRequest request) {
        return topicService.createTopic(request);
    }

    @GetMapping
    public List<TopicResponse> list() {
        return topicService.listTopics();
    }

    @PostMapping("/{topic}/messages")
    public MessageResponse publish(
            @PathVariable String topic,
            @Valid @RequestBody PublishMessageRequest request) {
        return messageService.publish(topic, request.key(), request.value());
    }

    @GetMapping("/{topic}/messages")
    public List<MessageResponse> messages(
            @PathVariable String topic,
            @RequestParam(required = false) Integer partition) {
        var metadata = topicService.requireTopic(topic);
        if (partition != null
                && (partition < 0 || partition >= metadata.getPartitionCount())) {
            throw new IllegalArgumentException(
                    "partition must be between 0 and " + (metadata.getPartitionCount() - 1));
        }
        return messageService.listMessages(topic, partition);
    }
}
