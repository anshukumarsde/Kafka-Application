package com.example.kafkaapplication.config;

import com.example.kafkaapplication.dto.CreateTopicRequest;
import com.example.kafkaapplication.service.TopicService;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DemoDataInitializer {
    @Bean
    ApplicationRunner createOrdersTopic(TopicService topicService) {
        return args -> topicService.createTopic(new CreateTopicRequest("orders", 3));
    }
}
