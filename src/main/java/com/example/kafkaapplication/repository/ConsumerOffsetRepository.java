package com.example.kafkaapplication.repository;

import com.example.kafkaapplication.entity.ConsumerOffset;
import com.example.kafkaapplication.entity.ConsumerOffsetId;
import org.springframework.data.jpa.repository.JpaRepository;
public interface ConsumerOffsetRepository
        extends JpaRepository<ConsumerOffset, ConsumerOffsetId> {
}
