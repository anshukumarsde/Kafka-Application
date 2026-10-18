package com.example.kafkaapplication.repository;

import com.example.kafkaapplication.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, Long> {
    List<ProcessedEvent> findAllByGroupIdOrderByIdAsc(String groupId);
}
