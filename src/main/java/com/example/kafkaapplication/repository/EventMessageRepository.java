package com.example.kafkaapplication.repository;

import com.example.kafkaapplication.entity.EventMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;

public interface EventMessageRepository extends JpaRepository<EventMessage, Long> {
    @Query("""
            select coalesce(max(message.offset), -1)
            from EventMessage message
            where message.topicName = :topic and message.partition = :partition
            """)
    long findLastOffset(@Param("topic") String topic, @Param("partition") int partition);

    Optional<EventMessage> findFirstByTopicNameAndPartitionAndOffset(
            String topicName, int partition, long offset);

    List<EventMessage> findAllByTopicNameOrderByPartitionAscOffsetAsc(String topicName);

    List<EventMessage> findAllByTopicNameAndPartitionOrderByOffsetAsc(
            String topicName, int partition);
}
