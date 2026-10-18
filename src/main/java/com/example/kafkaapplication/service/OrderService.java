package com.example.kafkaapplication.service;

import com.example.kafkaapplication.dto.OrderResponse;
import com.example.kafkaapplication.entity.Order;
import com.example.kafkaapplication.entity.OutboxEvent;
import com.example.kafkaapplication.exception.ConflictException;
import com.example.kafkaapplication.repository.OrderRepository;
import com.example.kafkaapplication.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final TopicService topicService;
    private final ObjectMapper objectMapper;

    public OrderService(
            OrderRepository orderRepository,
            OutboxEventRepository outboxRepository,
            TopicService topicService,
            ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.topicService = topicService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderResponse create(String orderId) {
        if (orderRepository.existsById(orderId)) {
            throw new ConflictException("Order already exists: " + orderId);
        }
        topicService.ensureTopic("orders", 3);
        Order order = orderRepository.save(new Order(orderId, "created"));
        try {
            String eventJson = objectMapper.writeValueAsString(
                    new OrderCreatedEvent(orderId, "created"));
            outboxRepository.save(new OutboxEvent("orders", orderId, eventJson));
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize order event", exception);
        }
        return new OrderResponse(order.getOrderId(), order.getStatus());
    }

    private record OrderCreatedEvent(String orderId, String status) {
    }
}
