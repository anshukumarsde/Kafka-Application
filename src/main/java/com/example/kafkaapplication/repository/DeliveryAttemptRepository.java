package com.example.kafkaapplication.repository;

import com.example.kafkaapplication.entity.DeliveryAttempt;
import com.example.kafkaapplication.entity.DeliveryAttemptId;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeliveryAttemptRepository
        extends JpaRepository<DeliveryAttempt, DeliveryAttemptId> {
}
