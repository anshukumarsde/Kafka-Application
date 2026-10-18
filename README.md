# Kafka-Application

A small Java 17 / Spring Boot event-driven systems MVP. It uses a local H2 database to make the topic log, consumer offsets, retries, and transactional outbox easy to run without a Kafka cluster. This is a layered teaching app, not a distributed Kafka replacement.

## Run

Install JDK 17+ and Maven, then run:

```powershell
mvn spring-boot:run
```

The service listens on `http://localhost:8080`. Data is stored in the local `data` directory. Run the integration tests with:

```powershell
mvn test
```

## Layers

- `controller`: HTTP endpoints and request validation.
- `service`: topic/message operations, consumer offset and retry logic, order creation, and outbox relay.
- `repository`: Spring Data JPA persistence interfaces.
- `entity`: persisted topic, message, consumer-group, offset, retry, order, and outbox records.
- `dto`: request and response contracts.

## API walkthrough

The `orders` topic is created at startup with three partitions.

```powershell
# Create topic and group
curl.exe -X POST http://localhost:8080/api/topics -H "Content-Type: application/json" -d "{\"name\":\"payments\",\"partitions\":3}"
curl.exe -X POST http://localhost:8080/api/consumer-groups -H "Content-Type: application/json" -d "{\"groupId\":\"billing\"}"

# Publish; messages sharing a key are assigned to the same partition
curl.exe -X POST http://localhost:8080/api/topics/payments/messages -H "Content-Type: application/json" -d "{\"key\":\"payment-42\",\"value\":{\"amount\":25}}"

# Consume one message, inspect group offsets, and inspect successfully processed events
curl.exe -X POST "http://localhost:8080/api/consumer-groups/billing/topics/payments/consume?maxAttempts=3"
curl.exe http://localhost:8080/api/consumer-groups/billing/topics/payments/offsets
curl.exe http://localhost:8080/api/consumer-groups/billing/processed
```

Set `"simulateFailure": true` in a message value to exercise retries. A message that fails the configured number of attempts is appended to `<topic>.DLQ`, and the consumer group's source offset advances. A consumer cycles through available partitions, so a retry in one partition does not prevent it from processing another partition.

The transactional outbox example creates an order and its event in one database transaction. The relay runs automatically and can also be triggered manually:

```powershell
curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d "{\"orderId\":\"order-42\"}"
curl.exe -X POST http://localhost:8080/api/outbox/relay
```

Within the demo database, relaying the event and marking its outbox row published are atomic. Consumers should still be idempotent when using external side effects: real systems can redeliver after a crash between handler execution and offset commit. Ordering is guaranteed only within a partition, not across partitions. Consumer membership assignment/rebalancing and multi-instance distributed coordination are outside this MVP's scope.
