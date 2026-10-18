package com.example.kafkaapplication.repository;

import com.example.kafkaapplication.entity.ConsumerGroup;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsumerGroupRepository extends JpaRepository<ConsumerGroup, String> {
}
