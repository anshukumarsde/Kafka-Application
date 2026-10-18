package com.example.kafkaapplication.repository;

import com.example.kafkaapplication.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, String> {
}
