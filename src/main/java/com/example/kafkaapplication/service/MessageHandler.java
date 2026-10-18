package com.example.kafkaapplication.service;

import com.example.kafkaapplication.entity.EventMessage;

public interface MessageHandler {
    void handle(String groupId, EventMessage message);
}
