package com.example.kafkaapplication.repository;

import com.example.kafkaapplication.entity.ConsumerCursor;
import com.example.kafkaapplication.entity.ConsumerCursorId;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ConsumerCursorRepository
        extends JpaRepository<ConsumerCursor, ConsumerCursorId> {
}
