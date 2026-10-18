package com.example.kafkaapplication.controller;

import com.example.kafkaapplication.dto.ConsumeResultResponse;
import com.example.kafkaapplication.dto.ConsumerGroupResponse;
import com.example.kafkaapplication.dto.CreateConsumerGroupRequest;
import com.example.kafkaapplication.dto.OffsetResponse;
import com.example.kafkaapplication.dto.ProcessedEventResponse;
import com.example.kafkaapplication.service.ConsumerGroupService;
import com.example.kafkaapplication.service.ConsumerService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Validated
@RestController
@RequestMapping("/api/consumer-groups")
public class ConsumerGroupController {
    private final ConsumerGroupService consumerGroupService;
    private final ConsumerService consumerService;

    public ConsumerGroupController(
            ConsumerGroupService consumerGroupService,
            ConsumerService consumerService) {
        this.consumerGroupService = consumerGroupService;
        this.consumerService = consumerService;
    }

    @PostMapping
    public ConsumerGroupResponse create(@Valid @RequestBody CreateConsumerGroupRequest request) {
        return consumerGroupService.create(request.groupId());
    }

    @GetMapping
    public List<ConsumerGroupResponse> list() {
        return consumerGroupService.list();
    }

    @PostMapping("/{groupId}/topics/{topic}/consume")
    public ConsumeResultResponse consumeOne(
            @PathVariable String groupId,
            @PathVariable String topic,
            @RequestParam(defaultValue = "3") @Min(1) int maxAttempts) {
        return consumerService.consumeOne(groupId, topic, maxAttempts);
    }

    @GetMapping("/{groupId}/topics/{topic}/offsets")
    public List<OffsetResponse> offsets(
            @PathVariable String groupId,
            @PathVariable String topic) {
        return consumerService.offsets(groupId, topic);
    }

    @GetMapping("/{groupId}/processed")
    public List<ProcessedEventResponse> processed(@PathVariable String groupId) {
        return consumerGroupService.processed(groupId);
    }
}
