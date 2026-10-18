package com.example.kafkaapplication;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:event-test;DB_CLOSE_DELAY=-1",
        "app.outbox.relay-delay-ms=3600000"
})
@AutoConfigureMockMvc
@Transactional
class EventSystemIntegrationTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void publishesMessagesToStablePartitionsWithIncreasingOffsets() throws Exception {
        createTopic("payments", 3);

        JsonNode first = publish("payments", "payment-1", "{\"sequence\":1}");
        JsonNode second = publish("payments", "payment-1", "{\"sequence\":2}");

        assertThat(first.path("partition").asInt()).isEqualTo(second.path("partition").asInt());
        assertThat(first.path("offset").asLong()).isZero();
        assertThat(second.path("offset").asLong()).isEqualTo(1);
        mockMvc.perform(get("/api/topics/payments/messages")
                        .param("partition", first.path("partition").asText()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    @Test
    void consumerGroupsMaintainIndependentOffsets() throws Exception {
        createTopic("group-events", 2);
        publish("group-events", "key", "{\"value\":1}");
        createGroup("group-a");
        createGroup("group-b");

        consume("group-a", "group-events").andExpect(jsonPath("$.status").value("processed"));
        consume("group-b", "group-events").andExpect(jsonPath("$.status").value("processed"));
        mockMvc.perform(get("/api/consumer-groups/group-a/topics/group-events/offsets"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].nextOffset").value(org.hamcrest.Matchers.hasItem(1)));
        mockMvc.perform(get("/api/consumer-groups/group-b/processed"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1));
    }

    @Test
    void retriesFailedMessageThenCommitsItToDeadLetterTopic() throws Exception {
        createTopic("retry-events", 2);
        publish("retry-events", "bad-event", "{\"simulateFailure\":true}");
        createGroup("retry-worker");

        consume("retry-worker", "retry-events", 2)
                .andExpect(jsonPath("$.status").value("retry"))
                .andExpect(jsonPath("$.attempts").value(1));
        consume("retry-worker", "retry-events", 2)
                .andExpect(jsonPath("$.status").value("dead_lettered"))
                .andExpect(jsonPath("$.attempts").value(2));
        mockMvc.perform(get("/api/topics/retry-events.DLQ/messages"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].value.sourceOffset").value(0));
        mockMvc.perform(get("/api/consumer-groups/retry-worker/topics/retry-events/offsets"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[*].nextOffset").value(org.hamcrest.Matchers.hasItem(1)));
    }

    @Test
    void orderAndOutboxEventAreRelayedAtomically() throws Exception {
        mockMvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"orderId\":\"order-test\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("created"));

        mockMvc.perform(post("/api/outbox/relay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.published").value(1));
        mockMvc.perform(get("/api/topics/orders/messages"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].key").value("order-test"))
                .andExpect(jsonPath("$[0].value.orderId").value("order-test"));
        mockMvc.perform(post("/api/outbox/relay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.published").value(0));
    }

    @Test
    void invalidTopicAndPartitionRequestsAreRejected() throws Exception {
        mockMvc.perform(post("/api/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"invalid\",\"partitions\":0}"))
                .andExpect(status().isBadRequest());
        createTopic("small-topic", 1);
        mockMvc.perform(get("/api/topics/small-topic/messages").param("partition", "2"))
                .andExpect(status().isBadRequest());
    }

    private void createTopic(String name, int partitions) throws Exception {
        mockMvc.perform(post("/api/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                new TopicRequest(name, partitions))))
                .andExpect(status().isOk());
    }

    private JsonNode publish(String topic, String key, String value) throws Exception {
        String result = mockMvc.perform(post("/api/topics/{topic}/messages", topic)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(
                                Map.of("key", key, "value", objectMapper.readTree(value)))))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        return objectMapper.readTree(result);
    }

    private void createGroup(String groupId) throws Exception {
        mockMvc.perform(post("/api/consumer-groups")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"groupId\":\"" + groupId + "\"}"))
                .andExpect(status().isOk());
    }

    private org.springframework.test.web.servlet.ResultActions consume(
            String groupId, String topic) throws Exception {
        return consume(groupId, topic, 3);
    }

    private org.springframework.test.web.servlet.ResultActions consume(
            String groupId, String topic, int maxAttempts) throws Exception {
        return mockMvc.perform(post(
                        "/api/consumer-groups/{groupId}/topics/{topic}/consume",
                        groupId, topic)
                .param("maxAttempts", Integer.toString(maxAttempts)));
    }

    private record TopicRequest(String name, int partitions) {
    }
}
