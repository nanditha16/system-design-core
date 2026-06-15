### Notes
common   - shared code (like a library)
services - actual running apps
infrastructure -   Kafka, Redis, Postgres

### Inside services/:
api-gateway        (entry point - optional for now)
ingestion-service  (where requests enter)
processing-service (business logic engine)
state-service      (database owner)
reconciliation     (background checker)

### Chain reaction:
HTTP request
  → ingestion
    → Kafka
      → processing
        → Kafka
          → state (DB write)

### Patterns used:
    Event sourcing (light) --> Kafka topics
    CQRS --> write = events, read = API
    Idempotent consumer --> processed_events
    DLQ --> processing-service
    State machine --> TransactionEntity
    Exactly-once approximation --> DB + Kafka ack

#############################################################################################################

### Operational Notes

NOTE: Codespaces already has Java installed 
    openjdk version "21.0.7" 2025-04-15 LTS
    OpenJDK Runtime Environment Microsoft-11369942 (build 21.0.7+6-LTS)
    OpenJDK 64-Bit Server VM Microsoft-11369942 (build 21.0.7+6-LTS, mixed mode, sharing)  

Step 1: Start ONLY infrastructure
```
docker compose -f infrastructure/docker-compose.yml up -d
docker ps

docker compose -f infrastructure/docker-compose.yml down
```
You started:
    Kafka - messaging backbone [Kafka = message bus, Topics = channels]
    Redis - idempotency
    Postgres - database
    Zookeeper - Kafka dependency

Steps:
    1. Images downloaded: redis, postgres, zookeeper, kafka Pulled
    2. Containers running, as This is the infrastructure my services depend on
    3. Topic creation script runs and Created topic/(communication channels between services - stream or queue of messages)
        ```
        ./infrastructure/create-topics.sh
        
        docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
        ```
        Creates:
            transactions.incoming.v1 [new requests]
            transactions.processed.v1 [after business logic/processing]
            transactions.dlq.v1 [failed messages]
            reconciliation.discrepancies.v1 [audit problems]
    
Logs (ignore warning): 
```
    ./infrastructure/create-topics.sh
    Waiting for Kafka...
    Kafka ready.
    WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
    Created topic transactions.incoming.v1.
    WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
    Created topic transactions.processed.v1.
    WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
    Created topic transactions.dlq.v1.
    WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
    Created topic reconciliation.discrepancies.v1.
    Topics:
    reconciliation.discrepancies.v1
    transactions.dlq.v1
    transactions.incoming.v1
    transactions.processed.v1
    
    CONTAINER ID   IMAGE                             COMMAND                  CREATED          STATUS                    PORTS                                                             NAMES
6409fd08246d   confluentinc/cp-kafka:7.5.3       "/etc/confluent/dock…"   18 minutes ago   Up 18 minutes (healthy)   0.0.0.0:9092->9092/tcp, [::]:9092->9092/tcp                       kafka
fd3e61f0a686   confluentinc/cp-zookeeper:7.5.3   "/etc/confluent/dock…"   18 minutes ago   Up 18 minutes             2888/tcp, 0.0.0.0:2181->2181/tcp, [::]:2181->2181/tcp, 3888/tcp   zookeeper
cfa2697c7a4b   postgres:16-alpine                "docker-entrypoint.s…"   18 minutes ago   Up 18 minutes             0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp                       postgres
abed20accffb   redis:7-alpine                    "docker-entrypoint.s…"   18 minutes ago   Up 18 minutes             0.0.0.0:6379->6379/tcp, [::]:6379->6379/tcp                       redis
```

#############################################################################################################

Step 2 — Build entire project
    - It is multi-module Maven project
    - project root: <packaging>pom</packaging> system-design-core
    system-design-core
     ├── common - The common module is NOT on Maven Central. It’s local to your repo
     
     - Case 1: build app → publish to your local Maven “mini store”
        - Run upto install phase: compile, test (optional), package JAR, install into local Maven repo
        - first time setting up project, fixing dependency issues, working with multi-module repo
        - Compiles everything, Installs common into local Maven repo (~/.m2), Now other modules can find it.
    ```
        mvn -DskipTests install
    ```
    
    - Case 2: build app → keep it locally (private)
        - This runs Maven phases up to package, i.e compile code, run tests (unless skipped), create JAR file quiet (less logs) don’t run tests
        - Results in : target/your-app.jar that stays inside the module only
        - package does NOT make the artifact available to other modules
        - building deployable jar, CI/CD pipelines
        ```
            mvn -q -DskipTests package
        ```
    Logs:
    ```
        build_logs.md
    ```
#############################################################################################################

Step 2 — Start ONE service  - INGESTION + KAFA MESSGAE validation (Request accepted, Event sitting in Kafka)
        - ingestion does NOT process business logic/ Write to DB/ Complete transaction
        - It ONLY: Accept request, Deduplicate (Redis), Send to Kafka 
        - Netty started on port 8081. It’s using WebFlux (reactive), Not Tomcat (blocking)
        - ingestion = high-throughput, non-blocking
        - request → queue → later processing
        - Use -am: running ONE service in dev, don’t want full build every time
        - POST /transactions
            → Redis check 
            → Kafka publish (PENDING)
    - Step 2.1 - Start ONLY ingestion: - http://localhost:8081
        ```
        mvn -pl services/ingestion-service spring-boot:run
        ```
        
    - Step 2.2 - Test: Send FIRST request
        ```
        curl -X POST localhost:8081/api/v1/transactions \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: k1" \
          -d '{"accountId":"a1","amount":100,"currency":"USD","region":"US","type":"PAYMENT"}'
      
      Response: 202 ACCEPTED
        {"transactionId":"cf2abd6a-272a-4f1b-aea6-62b26831f53c","status":"PENDING","message":"accepted"}

        ```
        - 1. Controller hit When someone POSTs a transaction, run this method: TransactionController.submit()
        - 2. Idempotency check: idempotency.reserve("k1") 
                Redis: SETNX idem:k1
        - 3. Event created: if publishNew else retunr HTTP 409  
                TransactionEvent(
                  status = PENDING
                )
        - 4. Sent to Kafka: publisher.publish(event)  [KafkaEventPublisher.publish()]
                message is now in Kafka,
                    Topic: transactions.incoming.v1
        - 5. Response returned - 202 ACCEPTED: ResponseEntity.accepted()
        
    - Step 2.3 — Verify Kafka has your message the output of transactions.incoming.v1
        ```
        docker exec kafka kafka-console-consumer \
          --bootstrap-server kafka:29092 \
          --topic transactions.incoming.v1 \
          --from-beginning
          
        {"eventId":"5615a41a-e414-4bd3-9523-ae9b6eeac581","transactionId":"6cc49923-e92c-41cd-a520-2db25eedf10e","idempotencyKey":"k100","amount":100,"currency":"USD","region":"US","type":"PAYMENT","status":"PENDING","occurredAt":1781372451.089589147}
          
        docker exec kafka kafka-console-consumer \
          --bootstrap-server kafka:29092 \
          --topic transactions.incoming.v1 \
          --from-beginning \
          --max-messages 5
          
        {"eventId":"f9179d62-4cea-426d-9c6e-60cc61bb43f0","transactionId":"cf2abd6a-272a-4f1b-aea6-62b26831f53c","idempotencyKey":"k1","amount":100,"currency":"USD","region":"US","type":"PAYMENT","status":"PENDING","occurredAt":1781362567.714518816}
            Processed a total of 1 messages
          
        ```
        Test idempotency: Run the SAME curl again:
        ```
        curl -X POST localhost:8081/api/v1/transactions \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: k1" \
          -d '{"accountId":"a1","amount":100,"currency":"USD","region":"US","type":"PAYMENT"}'

        {"transactionId":null,"status":"DUPLICATE","message":"Idempotency-Key already processed"}
        ```
    Logs:
    ```
        Ingestion_service_logs.md
            - ProducerConfig values: 
                acks = -1 (acks = all)  : Kafka waits until ALL replicas confirm write before success for High durability
                enable.idempotence = true : Kafka guarantees no duplicate messages on retries
                retries = 5 : If Kafka send fails: it retries up to 5 times
                max.in.flight.requests.per.connection = 5 : Controls how many unacknowledged requests are allowed for safe concurrency
                Kafka version: 3.6.2
            - Producer initialized successfully, No connection errors, Kafka reachable
                [Producer clientId=producer-1] Instantiated an idempotent producer.    
            - Cluster connection established, app connected to Kafka cluster 
                Cluster ID: zg4Ox7pCQ660iZde4-BWVA
            - Final confirmation: Kafka assigned producer identity, Ready to send messages
                ProducerId set to 0 with epoch 0

    ```
    
#############################################################################################################

Step 3 — Start ONE service  - PROCESSING
        - Spring Boot plugin to Run the processing-service module exactly, stateless compute engine [Queue → Worker → Queue: Message IN → Message OUT]
        - Netty started on port 8082
        - Kafka consumers work in groups, allows multiple instances, load balancing, horizontal scaling
            groupId = processing-service
        - Connected to Kafka, Pulled your message, Ran business logic, Sent new message (SENT)
        - Kafka delivers message
            → TransactionConsumer.onTransaction()
            → router.process()
            → change status to SENT
            → publish to processed topic
            → commit offset
    - Step 3.1 - Start ONLY processing: - localhost:9092
        ```
        cd services/processing-service
        mvn spring-boot:run
        ```
    - Step 3.2 - TEST: the output of transactions.processed.v1
    should be processed and placed in process topic
        ```
        docker exec kafka kafka-console-consumer \
          --bootstrap-server kafka:29092 \
          --topic transactions.processed.v1 \
          --from-beginning \
          --max-messages 5
          
        {"eventId":"5615a41a-e414-4bd3-9523-ae9b6eeac581","transactionId":"6cc49923-e92c-41cd-a520-2db25eedf10e","idempotencyKey":"k100","amount":100,"currency":"USD","region":"US","type":"PAYMENT","status":"SENT","occurredAt":1781372451.981793870}
        ```

    Logs:
    ```
        processing_service_logs.md:  Folloes Kafka consumption, Group coordination, Partition assignment, Event processing, Event transformation, Safe offset handling
            - ProcessingApplication started
            - ConsumerConfig values:  This is the Kafka consumer setup
                bootstrap.servers = localhost:9092 (connects to Kafka)
                group.id = processing-service (This defines the consumer group - All processing-service instances belong to one team)
                enable.auto.commit = false (manual control over offset, prevents data loss)
                auto.offset.reset = earliest (If no previous offset: start from beginning of topic. That is why old message got processed immediately)
                partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
                ssl.enabled.protocols = [TLSv1.2, TLSv1.3]
            - Subscribing to topic: now listening to incoming transactions
                [Consumer clientId=consumer-processing-service-1, groupId=processing-service] Subscribed to topic(s): transactions.incoming.v1
            - Kafka group coordination Happens every time a consumer starts - [Consumer clientId=consumer-processing-service-1, groupId=processing-service] 
                - ConsumerCoordinator: Group formed successfully, now ACTIVE consumer 
                    (Re-)joining group
                    Request joining group due to: need to re-join with the given member-id: consumer-processing-service-1-d97b37b8-41fa-4063-b307-216faeea234a
                    Request joining group due to: rebalance failed due to 'The group member needs to have a valid member id before actually entering a consumer group.' (MemberIdRequiredException)
                    Successfully joined group with generation Generation{generationId=1, memberId='consumer-processing-service-1-d97b37b8-41fa-4063-b307-216faeea234a', protocol='range'}
            - Partition assignment: Kafka topic has 6 partitions, Your service got ALL of them, This instance will process ALL messages
                - ConsumerCoordinator: PARTITIONS READY, system is ready to consume messages
                    Finished assignment for group at generation 1: {consumer-processing-service-1-d97b37b8-41fa-4063-b307-216faeea234a=Assignment(partitions=[transactions.incoming.v1-0, transactions.incoming.v1-1, transactions.incoming.v1-2, transactions.incoming.v1-3, transactions.incoming.v1-4, transactions.incoming.v1-5])}
                    Successfully synced group in generation Generation{generationId=1, memberId='consumer-processing-service-1-d97b37b8-41fa-4063-b307-216faeea234a', protocol='range'}
                    Notifying assignor about the new Assignment(partitions=[transactions.incoming.v1-0, transactions.incoming.v1-1, transactions.incoming.v1-2, transactions.incoming.v1-3, transactions.incoming.v1-4, transactions.incoming.v1-5])
                    Adding newly assigned partitions: transactions.incoming.v1-0, transactions.incoming.v1-1, transactions.incoming.v1-2, transactions.incoming.v1-3, transactions.incoming.v1-4, transactions.incoming.v1-5
            - Offset reset - Kafka says You've never processed anything before, so Start from beginning of topic
                - ConsumerCoordinator:
                    Found no committed offset for partition transactions.incoming.v1-4
                    Found no committed offset for partition transactions.incoming.v1-5
                    Found no committed offset for partition transactions.incoming.v1-0
                    Found no committed offset for partition transactions.incoming.v1-1
                    Found no committed offset for partition transactions.incoming.v1-2
                    Found no committed offset for partition transactions.incoming.v1-3
            - Kafka takes a moment to: assign partition, start polling
                - SubscriptionState: 
                    Resetting offset for partition transactions.incoming.v1-4 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
                    Resetting offset for partition transactions.incoming.v1-5 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
                    Resetting offset for partition transactions.incoming.v1-0 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
                    Resetting offset for partition transactions.incoming.v1-1 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
                    Resetting offset for partition transactions.incoming.v1-2 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
                    Resetting offset for partition transactions.incoming.v1-3 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
              - Processing consumed:  
                - KafkaMessageListenerContainer: 
                    processing-service: partitions assigned: [transactions.incoming.v1-0, transactions.incoming.v1-1, transactions.incoming.v1-2, transactions.incoming.v1-3, transactions.incoming.v1-4, transactions.incoming.v1-5]
            - Router executed: return event.withStatus(SENT);
                - TransactionRouter: TransactionRouter.process - successfully pulled from Kafka
                    Routing txn=cf2abd6a-272a-4f1b-aea6-62b26831f53c region=US type=PAYMENT
            - ProducerConfig values: Because processing-service does: consume → process → produce i.e  pipeline stage
                - New event published: no duplicates on retries
                    - ProducerConfig values and KafkaProducer:  Instantiated an idempotent producer. 
                - Offset committed: ack.acknowledge();
            - FINAL RESULT: changed from PENDING → SENT 
                Processed txn=cf2abd6a-272a-4f1b-aea6-62b26831f53c -> SENT
    ```

#############################################################################################################

Step 4 — Start ONE service  - STATE SERVICE - StateApplication
        - DB-backed service is alive with bootstrapping Spring Data JPA repositories in DEFAULT mode
        - Tomcat initialized with port 8083 (http)
        - constraint "uq_idem_key" does not exist, skipping initial run. Hibernate is trying to drop constraints that don’t exist yet
        - Auto-configures: Kafka, JPA, DataSource
        - Kafka consumer starts (@KafkaListener) onProcessed - transactions.processed.v1
            - Kafka delivers message, Manual ACK, Offset NOT committed until DB write succeeds, If failure → retried
        - TransactionStateService (stateService.apply(event)) 
            - Deduplication (event-level), if yes No DB changes, else insert into processed_events table (ledger of processed Kafka messages)
            - Kafka is at-least-once delivery
            - Upsert Transaction State (findByTransactionI), Transaction EXISTS else
                Transaction NEW: First time we see this transaction - single-writer model: owns and mutates transaction state
            - State Machine Enforcement (transitionTo(TransactionStatus next)) 
                Critical rule: Prevents invalid flows like: SUCCESS → PENDING, FAILED → SENT
                    if (!status.canTransitionTo(next))
                        throw IllegalStateException
            - Auto Completion (External settlement confirmation):  if (event.status() == SENT)  → transitionTo(SUCCESS) 
        - DATABASE DESIGN: 
            Table 1: processed_events (Same Kafka event never applied twice)
            Table 2: transactions (UniqueConstraint: transaction_id - Prevent duplicate events creating rows, idempotency_key - Prevent duplicate API submissions)
        - multi-layer idempotency:
            API (Redis): Duplicate request
            Kafka consumer: Duplicate event
            DB constraints: Absolute safety
        - TRANSACTIONAL BOUNDARY: What runs inside ONE DB transaction - Exactly-once approximation
            Insert into processed_events
            Insert/update transactions
           If failure happens: nothing committed, Kafka offset not acknowledged, -> message retried 
        - QUERY API: @GetMapping("/{transactionId}") -> repository.findByTransactionId(...)
             Returns current state from DB
             Example: 
             ```
             POST → ingestion → Kafka → processing → state-service
             ↓
             GET → database read
             ```
        - consume (SENT)
            → write to DB
            → enforce state machine
            → move to SUCCESS
      - Step 4.1 - Start ONLY processing: - localhost:9092
        ```
        cd services/state-service
        mvn spring-boot:run
        ```
      - Step 4.2 - TEST:  Kafka → Consumer → StateService → DB (atomic write) → Query API

        ```
            Check processed topic manually
            curl -X POST localhost:8081/api/v1/transactions \
               -H "Content-Type: application/json" \
               -H "Idempotency-Key: k100" \
               -d '{"accountId":"a1","amount":100,"currency":"USD","region":"US","type":"PAYMENT"}'
            
            {"transactionId":"6cc49923-e92c-41cd-a520-2db25eedf10e","status":"PENDING","message":"accepted"}
            
            docker exec kafka kafka-console-consumer \
                       --bootstrap-server kafka:29092 \
                       --topic transactions.incoming.v1 \
                       --from-beginning \
                       --max-messages 5
            {"eventId":"5615a41a-e414-4bd3-9523-ae9b6eeac581","transactionId":"6cc49923-e92c-41cd-a520-2db25eedf10e","idempotencyKey":"k100","amount":100,"currency":"USD","region":"US","type":"PAYMENT","status":"PENDING","occurredAt":1781372451.089589147}
            
            docker exec kafka kafka-console-consumer \
               --bootstrap-server kafka:29092 \
               --topic transactions.processed.v1 \
               --from-beginning \
               --max-messages 5

            {"eventId":"5615a41a-e414-4bd3-9523-ae9b6eeac581","transactionId":"6cc49923-e92c-41cd-a520-2db25eedf10e","idempotencyKey":"k100","amount":100,"currency":"USD","region":"US","type":"PAYMENT","status":"SENT","occurredAt":1781372451.981793870}
            
            # Terminal A - watch state-service logs live (it's already running, just watch)
            # Terminal B - send fresh transaction
            
            curl -s -X POST localhost:8081/api/v1/transactions \
               -H "Content-Type: application/json" \
               -H "Idempotency-Key: k200" \
               -d '{"accountId":"a1","amount":100,"currency":"USD","region":"US","type":"PAYMENT"}' | jq .
            {
              "transactionId": "c0e663c5-4246-4bbb-aadb-5a7a2aa2145b",
              "status": "PENDING",
              "message": "accepted"
            }
            
            curl -s localhost:8083/api/v1/transactions/c0e663c5-4246-4bbb-aadb-5a7a2aa2145b | jq .
            {
              "id": 2,
              "transactionId": "c0e663c5-4246-4bbb-aadb-5a7a2aa2145b",
              "status": "SUCCESS",
              "updatedAt": "2026-06-13T17:56:37.010209Z"
            }
            
            curl -s -X POST localhost:8081/api/v1/transactions \
               -H "Content-Type: application/json" \
               -H "Idempotency-Key: k200" \
               -d '{"accountId":"a1","amount":100,"currency":"USD","region":"US","type":"PAYMENT"}' | jq .
            {
              "transactionId": null,
              "status": "DUPLICATE",
              "message": "Idempotency-Key already processed"
            }
            
            curl -s -X POST localhost:8081/api/v1/transactions \
               -H "Content-Type: application/json" \
               -H "Idempotency-Key: k-bad" \
               -d '{"accountId":"a1","amount":-1,"currency":"USD","region":"US","type":"PAYMENT"}' | jq .
            {
              "transactionId": "936293cf-ce52-4e54-872e-131c78c73673",
              "status": "PENDING",
              "message": "accepted"
            }
            
            docker exec kafka kafka-console-consumer \
               --bootstrap-server kafka:29092 \
               --topic transactions.dlq.v1 --from-beginning --max-messages 1

            {"eventId":"606dbc75-49bf-4475-ba10-d7d60500b1e6","transactionId":"936293cf-ce52-4e54-872e-131c78c73673","idempotencyKey":"k-bad","amount":-1,"currency":"USD","region":"US","type":"PAYMENT","status":"PENDING","occurredAt":1781373808.715730512}
            Processed a total of 1 messages
        ```
    Logs:
        transactions.processed.v1 (Kafka topic) -> ProcessedEventConsumer (@KafkaListener) -> TransactionStateService.apply() -> Postgres: (processed_events (dedup ledger), transactions (state store)) -> GET API reads from DB
        E2E trace: event-driven system:
          Step 1: API call: POST /transactions (produces Kafka event (PENDING))
          Step 2: processing-service: PENDING → SENT (publishes to transactions.processed.v1)
          Step 3: state-service: Kafka message received → apply(), → DB write
          Step 4: Database: transactions table - for transaction_id, status = SUCCESS
          Step 5: Query: GET /transactions/abc123 → SUCCESS
    ```
        state_service_logs.md:  
            - Tomcat initialized with port 8083
            - HHH000204: Processing PersistenceUnitInfo [name: default], Hibernate ORM core version 6.4.4.Final
            - ConsumerConfig values: 
                auto.offset.reset = earliest (offsets are tied to consumer group, group.id = state-service, If anything consumed earlier OR offset already committed:it may skip processing logs)
            - ExtendedKafkaConsumer: Subscribed to topic(s): transactions.processed.v1,  Started StateApplicatio
                Cluster ID: zg4Ox7pCQ660iZde4-BWVA
                Discovered group coordinator localhost:9092 (id: 2147483646 rack: null)
              - ConsumerCoordinator - Consumer group coordination for joining consumer group
                    joining group
                    Request joining group due to: need to re-join with the given member-id: consumer-state-service-1-80def163-3df7-4770-a7d4-cc666bdaf5a1
                    Request joining group due to: rebalance failed due to 'The group member needs to have a valid member id before actually entering a consumer group.' (MemberIdRequiredException)
                    joining group
                    Successfully joined group with generation Generation{generationId=1, memberId='consumer-state-service-1-80def163-3df7-4770-a7d4-cc666bdaf5a1', protocol='range'}
            - Assignment & Partition assignment: getting assigned partitions
                 Finished assignment for group at generation 1: {consumer-state-service-1-80def163-3df7-4770-a7d4-cc666bdaf5a1=Assignment(partitions=[transactions.processed.v1-0, transactions.processed.v1-1, transactions.processed.v1-2, transactions.processed.v1-3, transactions.processed.v1-4, transactions.processed.v1-5])}
                 Successfully synced group in generation Generation{generationId=1, memberId='consumer-state-service-1-80def163-3df7-4770-a7d4-cc666bdaf5a1', protocol='range'}
                - Partition: 6 partitions
                     Notifying assignor about the new Assignment(partitions=[transactions.processed.v1-0, transactions.processed.v1-1, transactions.processed.v1-2, transactions.processed.v1-3, transactions.processed.v1-4, transactions.processed.v1-5])
                     Adding newly assigned partitions: transactions.processed.v1-0, transactions.processed.v1-1, transactions.processed.v1-2, transactions.processed.v1-3, transactions.processed.v1-4, transactions.processed.v1-5
            - Offset reset: - You've never consumed this topic before so Start from beginning
                Found no committed offset for partition transactions.processed.v1-4
                Resetting offset for partition transactions.processed.v1-4 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[localhost:9092 (id: 1 rack: null)], epoch=0}}.
            - KafkaMessageListenerContainer: 
                state-service: partitions assigned: [transactions.processed.v1-0, transactions.processed.v1-1, transactions.processed.v1-2, transactions.processed.v1-3, transactions.processed.v1-4, transactions.processed.v1-5]
            - [Tomcat].[localhost].[/]:
                Initializing Spring DispatcherServlet 'dispatcherServlet' and Completed initialization.
    ```
    
#############################################################################################################

Step 5 — Start ONE service  - RECONCILIATION SERVICE - ReconciliationApplication      
        - watch logs — within 30s it will flag the failed txn on reconciliation.discrepancies.v1
        - background auditor. It audits DB state, not Kafka backlog
        -  i,e publish to Kafka → Publishes To this topic: reconciliation.discrepancies.v1
        - So reconciliation does this: 
            Scan database -> Find inconsistent / stuck transactions -> Emit discrepancy events
            If no issues: Reconciliation pass clean: no stuck transactions 
        - On Application startup - JPA (DB access), Scheduler, Kafka producer (for discrepancies)
            - Scheduled Job: [c.c.r.job.ReconciliationJob] - Runs periodically (e.g. every 30s)
            - scheduler thread → reconciliation logic:
                (Query DB - findByStatusInAndUpdatedAtBefore) i.e Find transactions that are: in “in-progress” states (PENDING, SENT, etc.), but too old
                (Detection logic): The job identifies - this transaction should have completed by now.
                    Example: 
                        PENDING too long -> ingestion stuck, 
                        SENT but no SUCCESS -> state-service issue, 
                        missing entirely -> data loss, 
                        bad transaction → DLQ (not stuck)
                
      - Step 5.1 - Start ONLY processing: - 
        ```
        cd services/reconciliation-service && mvn spring-boot:run
        ```
      - Step 5.2 - TEST:  Kafka → Consumer → StateService → DB (atomic write) → Query API
            - stopped State service
            - new transaction sent
        ```
        curl -s -X POST localhost:8081/api/v1/transactions \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: k900" \
      -d '{"accountId":"a1","amount":100,"currency":"USD","region":"US","type":"PAYMENT"}' | jq .

          docker exec kafka kafka-console-consumer \
          --topic reconciliation.discrepancies.v1 \
          --bootstrap-server kafka:29092 \
          --from-beginning \
          --max-messages 1
        {"transactionId":"9e5e5814-2e2a-42f4-97c3-af1c28e6654b","status":"PENDING","detectedAt":"2026-06-14T16:50:47.728939418Z","stuckSince":"2026-06-14T16:40:32.132988Z"}
        Processed a total of 1 messages

        ```
        MANUAL TEST:
        ```
         docker exec -it postgres psql -U core -d coredb
            psql (16.14)
            Type "help" for help.

            coredb=# SELECT transaction_id, status, updated_at FROM transactions;
                        transaction_id            | status  |          updated_at           
            --------------------------------------+---------+-------------------------------
             6cc49923-e92c-41cd-a520-2db25eedf10e | SUCCESS | 2026-06-13 17:40:52.669558+00
             c0e663c5-4246-4bbb-aadb-5a7a2aa2145b | SUCCESS | 2026-06-13 17:56:37.010209+00
             7fb89b22-4d11-42f0-8fe9-8f6838ea6285 | SUCCESS | 2026-06-14 16:33:41.529313+00
             efef61e5-d86d-45ce-a122-65ec7051b051 | SUCCESS | 2026-06-14 16:39:39.691567+00
             2a58ce92-11f5-4ddf-b64a-3292efaa00f6 | SUCCESS | 2026-06-14 16:47:09.919533+00
             9e5e5814-2e2a-42f4-97c3-af1c28e6654b | SUCCESS | 2026-06-14 16:48:46.646947+00
            (6 rows)

            UPDATE transactions
            SET status = 'SUCCESS',
                updated_at = now() - interval '10 minutes'
            WHERE transaction_id = '9e5e5814-2e2a-42f4-97c3-af1c28e6654b';
            
            SELECT transaction_id, status, updated_at FROM transactions;
            
            coredb=# UPDATE transactions
            coredb-# SET status = 'PENDING',
            coredb-#     updated_at = now() - interval '10 minutes'
            coredb-# WHERE transaction_id = '9e5e5814-2e2a-42f4-97c3-af1c28e6654b';
            UPDATE 1
            coredb=# SELECT transaction_id, status, updated_at FROM transactions;
                        transaction_id            | status  |          updated_at           
            --------------------------------------+---------+-------------------------------
             6cc49923-e92c-41cd-a520-2db25eedf10e | SUCCESS | 2026-06-13 17:40:52.669558+00
             c0e663c5-4246-4bbb-aadb-5a7a2aa2145b | SUCCESS | 2026-06-13 17:56:37.010209+00
             7fb89b22-4d11-42f0-8fe9-8f6838ea6285 | SUCCESS | 2026-06-14 16:33:41.529313+00
             efef61e5-d86d-45ce-a122-65ec7051b051 | SUCCESS | 2026-06-14 16:39:39.691567+00
             2a58ce92-11f5-4ddf-b64a-3292efaa00f6 | SUCCESS | 2026-06-14 16:47:09.919533+00
             9e5e5814-2e2a-42f4-97c3-af1c28e6654b | PENDING | 2026-06-14 16:40:32.132988+00
            (6 rows)

            coredb=# 
        
        ```
    Logs:
        reconciliation_service_logs.md - Discrepancy event was sent to Kafka
            - Scheduler → runs every 30 sec : Reconciliation detected the issue - detect → emit → detect → emit → detect → emit (Repeated alerting / duplicate reconciliation events)
            - Event emission: Reconciliation → Kafka publish 
            - System observability: Silent failure → surfaced, does NOT track already-reported discrepancies
            Reconciliation found 1 stuck transactions - DB inconsistency → detected
    ```
        state_service_logs.md:  
           
    ```
   
#############################################################################################################

PHASE 2 - Reusable architecture template to apply it to a real business domain
    - what NOT changes: Kafka wiring, idempotency Redis layer, offset-after-write, DefaultErrorHandler+DLQ, reconciliation scheduler, devcontainer, docker-compose. 
        All structural guarantees carry over free.
    - What changes: Reused the exactly-once transaction pipeline and swap the domain model. 
        Step 1: Create the branch and folder structure
        Step 2: Add domain models to common
        Step 3: Add business topics to the create-topics script, when applicable
        Step 4: Add logic under each service correspondingly. 
            The rule: every use case gets its own subpackage named after the domain inside each service. 
                Generic code stays at the existing level. New use case = new subpackage, zero changes to existing code.
            
Examples:
    1. 
    ```
    git checkout -b feature/brokerage-system
    # from repo root, on feature/brokerage-system branch
    BASE=services

    for svc in ingestion-service processing-service state-service reconciliation-service; do
      pkg=$(echo $svc | tr '-' '/')
      # find the java source root
      src=$(find $BASE/$svc/src/main/java -type d -name "$(echo $svc | tr '-' '' | sed 's/service//')*" 2>/dev/null | head -1)
    done

    # Easier: just create directly
    mkdir -p services/ingestion-service/src/main/java/com/coresys/ingestion/brokerage/api
    mkdir -p services/processing-service/src/main/java/com/coresys/processing/brokerage/consumer
    mkdir -p services/processing-service/src/main/java/com/coresys/processing/brokerage/routing
    mkdir -p services/state-service/src/main/java/com/coresys/state/brokerage/domain
    mkdir -p services/state-service/src/main/java/com/coresys/state/brokerage/consumer
    mkdir -p services/state-service/src/main/java/com/coresys/state/brokerage/service
    mkdir -p services/state-service/src/main/java/com/coresys/state/brokerage/api
    mkdir -p services/reconciliation-service/src/main/java/com/coresys/reconciliation/brokerage/job
    mkdir -p common/src/main/java/com/coresys/common/events/brokerage
```

    2. mkdir -p common/src/main/java/com/coresys/common/events/brokerage
        OrderEvent.java
        OrderType.java
        OrderStatus.java
        BrokerageTopics.java
    3. Append to infrastructure/create-topics.sh
    4. Business Logic Implementation as per the struture rule: Starting with ingestion → processing → state → reconciliation.
        Step 1: services/ingestion-service/.../com/coresys/ingestion/
            └── brokerage/                       ← brokerage domain
            └── api/
                └── OrderController.java     (BUY/SELL endpoint)
        Step 2: services/processing-service/.../com/coresys/processing/
            └── brokerage/                       ← brokerage domain
            ├── consumer/
            │   └── OrderConsumer.java
            └── routing/
                └── OrderRouter.java
        Step 3: services/state-service/.../com/coresys/state/
            └── brokerage/                       ← brokerage domain
            ├── domain/
            │   ├── OrderEntity.java
            │   ├── AccountEntity.java
            │   ├── OrderRepository.java
            │   └── AccountRepository.java
            ├── consumer/
            │   └── OrderEventConsumer.java
            ├── service/
            │   └── OrderStateService.java
            └── api/
                └── OrderQueryController.java
        Step 4: services/reconciliation-service/.../com/coresys/reconciliation/
            └── brokerage/                       ← brokerage domain
                └── job/
                    └── OrderReconciliationJob.java
    5. Seed test data, one time.
    Example:
    ```
    docker exec -it postgres psql -U core -d coredb -c \
      "INSERT INTO accounts (user_id, balance) VALUES ('user-42', 10000.00) ON CONFLICT DO NOTHING;"
    ```
    6. Rebuild and restart all services
        API:  User places order - POST /api/v1/orders
            OrderController: idempotency check, create OrderEvent, publish → Kafka 
            Processing service: plug for market hours, risk checks, margin checks
                (orders.incoming → OrderConsumer) -> (router.process(event) → EXECUTING / → publish → orders.processed)
            State service: (orders.processed → OrderEventConsumer) -> stateService.apply(event)
                Inside: All inside one transaction
                    BUY --> account.debit(amount), save order (EXECUTED)
                    SELL --> account.credit(amount), save order
                What one ACID transaction now guarantees:
                    debit cash  +  credit shares  +  write order row  = ONE commit
                    debit shares + credit cash   +  write order row  = ONE commit
             Reconciliation service: scan DB every 30s, detect stuck orders, publish discrepancy 
                - Also add the two new recon.order.* properties to reconciliation-service/src/main/resources/application.yml:
    7. Testing:
        - (e.g. to reset for a clean demo), flush Redis first
        ```
        docker exec -it redis redis-cli FLUSHALL
        ```
        - Infra test:
        ```
        docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
            __consumer_offsets
            orders.discrepancies.v1
            orders.dlq.v1
            orders.incoming.v1
            orders.processed.v1
            reconciliation.discrepancies.v1
            transactions.dlq.v1
            transactions.incoming.v1
            transactions.processed.v1
        ```
        - DB has tables
        ```
         docker exec -it postgres psql -U core -d coredb
            SELECT * FROM accounts;
            SELECT * FROM orders;
            
            INSERT INTO accounts(user_id, balance, updated_at)
            VALUES ('u1', 10000, now());
            
            INSERT INTO accounts (user_id, balance) VALUES ('user-42', 10000.00) ON CONFLICT DO NOTHING;
  
            coredb=# INSERT INTO accounts(user_id, balance, updated_at)
            coredb-# VALUES ('u1', 10000, now());
            INSERT 0 1
            
            coredb=# SELECT * FROM accounts;
             user_id |  balance   |          updated_at           
            ---------+------------+-------------------------------
             u1      | 10000.0000 | 2026-06-14 18:22:35.249831+00
            (1 row)

            coredb=# SELECT * FROM orders;
             id | idempotency_key | order_id | price | quantity | status | symbol | type | updated_at | user_id 
            ----+-----------------+----------+-------+----------+--------+--------+------+------------+---------
            (0 rows)
            
        After Buy:
            coredb=# SELECT * FROM accounts;
             user_id |  balance  |          updated_at           
            ---------+-----------+-------------------------------
             u1      | 9000.0000 | 2026-06-14 18:24:27.097762+00
            (1 row)

            coredb=# SELECT order_id, user_id, status FROM orders;
                           order_id               | user_id |  status  
            --------------------------------------+---------+----------
             0da5d6e8-1abb-41e2-b56c-affd34ab9ac6 | u1      | EXECUTED
            (1 row)
        ```
        - Test the BUY flow
        ```
        curl -s -X POST localhost:8081/api/v1/orders \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: buy-aapl-001" \
          -d '{"userId":"user-42","type":"BUY","symbol":"AAPL","quantity":2,"price":150.00}' | jq .
        Response:     
            {
              "orderId": "1199a1eb-8245-439c-b3c2-14954eaf46c4",
              "status": "PENDING",
              "message": "accepted"
            }

        curl -s -X POST localhost:8081/api/v1/orders \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: k-b1" \
          -d '{
                "userId": "u1",
                "type": "BUY",
                "symbol": "AAPL",
                "quantity": 10,
                "price": 100
              }' | jq .
        Response:     
            {
              "orderId": "0da5d6e8-1abb-41e2-b56c-affd34ab9ac6",
              "status": "PENDING",
              "message": "accepted"
            }

        # Verify account exists
        curl -s localhost:8083/api/v1/brokerage/accounts/user-42 | jq .
        Response: 
            {
              "userId": "user-42",
              "balance": 10000,
              "updatedAt": "2026-06-14T18:29:00.200205Z"
            }

        # BUY 2 shares of AAPL at $150 - DUPLICATE check if already exists
        curl -s -X POST localhost:8081/api/v1/orders \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: buy-aapl-001" \
          -d '{"userId":"user-42","type":"BUY","symbol":"AAPL","quantity":2,"price":150.00}' | jq .
        Response:       
            {
              "orderId": null,
              "status": "DUPLICATE",
              "message": "Idempotency-Key already processed"
            }
            
        curl -s -X POST localhost:8081/api/v1/orders \
           -H "Content-Type: application/json" \
           -H "Idempotency-Key: buy-aapl-002" \
           -d '{"userId":"user-42","type":"BUY","symbol":"AAPL","quantity":2,"price":150.00}' | jq .
        Response:   
            {
              "orderId": "61df0d78-a153-4378-8b97-1c1798673db3",
              "status": "PENDING",
              "message": "accepted"
            }
        
        # check balance 
        sleep 3 && curl -s localhost:8083/api/v1/brokerage/accounts/user-42 | jq .
        Response: 
            {
              "userId": "user-42",
              "balance": 9700,
              "updatedAt": "2026-06-14T18:32:32.894295Z"
            }
        
        # Check holdings (should show AAPL qty=2)
        curl -s localhost:8083/api/v1/brokerage/accounts/user-42/holdings | jq .
        Response: 
            [
              {
                "userId": "user-42",
                "symbol": "AAPL",
                "quantity": 2,
                "updatedAt": "2026-06-14T18:32:32.899673Z"
              }
            ]

        # SELL 1 share of AAPL at $175 (profit)
        curl -s -X POST localhost:8081/api/v1/orders \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: sell-aapl-001" \
          -d '{"userId":"user-42","type":"SELL","symbol":"AAPL","quantity":1,"price":175.00}' | jq .
        Response: 
            {
              "orderId": "4a91add1-81d4-4d65-a074-a1e0421b84e8",
              "status": "PENDING",
              "message": "accepted"
            }

        # Check balance (should be 9875.00) and holdings (AAPL qty=1)
        curl -s localhost:8083/api/v1/brokerage/accounts/user-42 | jq .
        Response: 
            {
              "userId": "user-42",
              "balance": 9875,
              "updatedAt": "2026-06-14T18:36:25.267260Z"
            }

        # Idempotency proof — replay buy-aapl-001, expect 409
        curl -s -X POST localhost:8081/api/v1/orders \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: buy-aapl-001" \
          -d '{"userId":"user-42","type":"BUY","symbol":"AAPL","quantity":2,"price":150.00}' | jq .
         Response:        
            {
              "orderId": null,
              "status": "DUPLICATE",
              "message": "Idempotency-Key already processed"
            }
        
        # Insufficient funds proof — try to buy more than balance allows
            # Step 1: Fresh idempotency key, amount way over current balance (9875)
            curl -s -X POST localhost:8081/api/v1/orders \
              -H "Content-Type: application/json" \
              -H "Idempotency-Key: buy-bust-002" \
              -d '{"userId":"user-42","type":"BUY","symbol":"TSLA","quantity":100,"price":500.00}' | jq .
            Response:          
                {
                  "orderId": "6fbd642c-28e0-459b-b8a6-690e6430cfb3",
                  "status": "PENDING",
                  "message": "accepted"
                }
            # Step 2: immediately watch state-service logs for the rejection:
            LOG Check: 
                 : Order REJECTED 6fbd642c-28e0-459b-b8a6-690e6430cfb3: Insufficient funds: balance=9875.0000 required=50000.00 user=user-42
            # Step 3:
                # Order should appear as REJECTED
                sleep 4 && curl -s localhost:8083/api/v1/brokerage/accounts/user-42/orders | \
                  jq '[.[] | {orderId, type, symbol, quantity, price, status}]'
                Response: 
                    [
                      {
                        "orderId": "4a91add1-81d4-4d65-a074-a1e0421b84e8",
                        "type": "SELL",
                        "symbol": "AAPL",
                        "quantity": 1,
                        "price": 175,
                        "status": "EXECUTED"
                      },
                      {
                        "orderId": "61df0d78-a153-4378-8b97-1c1798673db3",
                        "type": "BUY",
                        "symbol": "AAPL",
                        "quantity": 2,
                        "price": 150,
                        "status": "EXECUTED"
                      }
                    ]

                # Balance must be UNCHANGED at 9875.00 (ACID rollback proof)
                    - When debit() throws, the entire transaction rolls back — including the orders.save() call — so the REJECTED order row never exists. 
                    - The insufficient funds exception is caught and swallowed inside apply(), so no exception propagates to the error handler. So no exception propagates to the error handler.
                    - Rejected orders are a business outcome, not a failure. 
                    curl -s localhost:8083/api/v1/brokerage/accounts/user-42 | jq .
                Response: 
                    {
                      "userId": "user-42",
                      "balance": 9875,
                      "updatedAt": "2026-06-14T18:36:25.267260Z"
                    }

                # Holdings must be UNCHANGED at AAPL qty=1 (rollback proof)
                curl -s localhost:8083/api/v1/brokerage/accounts/user-42/holdings | jq .
                Response:
                [
                  {
                    "userId": "user-42",
                    "symbol": "AAPL",
                    "quantity": 1,
                    "updatedAt": "2026-06-14T18:36:25.267250Z"
                  }
                ]            
        
        # Check for rejected order: also n logs as before
        docker exec -it redis redis-cli FLUSHALL

        curl -s -X POST localhost:8081/api/v1/orders \
          -H "Content-Type: application/json" \
          -H "Idempotency-Key: buy-bust-004" \
          -d '{"userId":"user-42","type":"BUY","symbol":"TSLA","quantity":100,"price":500.00}' | jq .
        Response:
            {
              "orderId": "f2f7aedb-1af6-4b4a-ab42-c43782da9ee9",
              "status": "PENDING",
              "message": "accepted"
            }

        sleep 3 && docker exec kafka kafka-console-consumer \
          --bootstrap-server kafka:29092 \
          --topic orders.rejected.v1 --from-beginning --max-messages 1
        Response:   
           {"orderId":"f2f7aedb-1af6-4b4a-ab42-c43782da9ee9","userId":"user-42","symbol":"TSLA","type":"BUY","amount":"50000.00","reason":"Insufficient funds: balance=9875.0000 required=50000.00 user=user-42","rejectedAt":"2026-06-14T19:03:38.657146612Z"}
        Processed a total of 1 messages     
             
        # Check order history
        curl -s localhost:8083/api/v1/brokerage/accounts/user-42/orders | jq '[.[] | {orderId, type: .type, status, updatedAt}]'
        Response: 
            [
              {
                "orderId": "4a91add1-81d4-4d65-a074-a1e0421b84e8",
                "type": null,
                "status": "EXECUTED",
                "updatedAt": "2026-06-14T18:36:25.267263Z"
              },
              {
                "orderId": "61df0d78-a153-4378-8b97-1c1798673db3",
                "type": null,
                "status": "EXECUTED",
                "updatedAt": "2026-06-14T18:32:32.904195Z"
              }
            ]
        ```

#############################################################################################################

Example 2: Banking Reconciliation System
    - What reuses unchanged: 
        Kafka wiring, Redis idempotency, offset-after-write, DLQ/retry, reconciliation scheduler pattern, devcontainer, docker-compose.
    - What's new:
        PaymentEntity with PENDING → SENT → CONFIRMED/FAILED state machine
        payment-events topic fan-out to 3 consumer groups
        Webhook handler (Core Banking callback)
        LocalStack S3 in docker-compose for local) audit sink
        EOD batch reconciliation (scheduled Spring Batch job that reads from DB + S3 and produces a discrepancy report)
        mTLS config seam (documented, not fully wired — cert management is infra not app code)
    - Structure: 
        - Branch
        ```
            git checkout -b feature/banking-reconciliation
            # from repo root, on feature/banking-reconciliation branch
            
            system-design-core/
            ├── common/src/main/java/com/coresys/common/events/
            │   └── banking/                                          ← NEW
            │       ├── BankingTopics.java
            │       ├── PaymentEvent.java
            │       ├── PaymentRegion.java
            │       ├── PaymentStatus.java
            │       └── PaymentType.java
            │
            ├── services/
            │   ├── ingestion-service/src/main/java/com/coresys/ingestion/
            │   │   ├── banking/                                      ← NEW
            │   │   │   ├── api/
            │   │   │   │   ├── PaymentController.java
            │   │   │   │   ├── PaymentRequest.java
            │   │   │   │   └── PaymentResponse.java
            │   │   │   └── webhook/
            │   │   │       ├── WebhookController.java
            │   │   │       └── WebhookRequest.java
            │   │   └── feature/publish/
            │   │       └── BankingEventPublisher.java                ← NEW
            │   │
            │   ├── processing-service/src/main/java/com/coresys/processing/
            │   │   └── banking/                                      ← NEW
            │   │       ├── consumer/
            │   │       │   └── PaymentConsumer.java
            │   │       ├── corebanking/
            │   │       │   └── CoreBankingClient.java                (mTLS seam)
            │   │       └── routing/
            │   │           └── PaymentRouter.java
            │   │
            │   ├── state-service/src/main/java/com/coresys/state/
            │   │   └── banking/                                      ← NEW
            │   │       ├── api/
            │   │       │   └── PaymentQueryController.java
            │   │       ├── consumer/
            │   │       │   ├── PaymentEventConsumer.java             (webhook results)
            │   │       │   └── PaymentRoutedConsumer.java            (routing results)
            │   │       ├── domain/
            │   │       │   ├── PaymentEntity.java
            │   │       │   └── PaymentRepository.java
            │   │       └── service/
            │   │           └── PaymentStateService.java
            │   │
            │   └── reconciliation-service/src/main/java/com/coresys/reconciliation/
            │       └── banking/                                      ← NEW
            │           ├── job/
            │           │   └── EodReconciliationJob.java             (MISSING_IN_CORE + STUCK_PENDING)
            │           └── sink/
            │               └── AuditSinkConsumer.java                (S3/HDFS data lake sim)
            │
            └── infrastructure/
                └── create-topics.sh                                  ← APPENDED (6 new banking topics)
        ```
        - Create topics and rebuild
        ```
            ./infrastructure/create-topics.sh
                banking.payments.incoming.v1
                banking.payments.routed.v1
                banking.payment.events.v1      ← fan-out (3 consumer groups)
                banking.payments.dlq.v1
                banking.payments.retry.v1
                banking.recon.discrepancies.v1

            mvn -DskipTests install
        ```
        - Test the full flow
            - processing-service: profile banking → only banking-router subscribed 
            - state-service: profile banking → only banking-state-manager + banking-state-routed subscribed 
            - reconciliation-service: profile banking → EodReconciliationJob + banking-audit-sink active 
            - Data flow:
                POST /banking/payments (ingestion:8081)
                  → Redis SETNX (idempotency reserved)
                  → banking.payments.incoming.v1 (Kafka)
                    → processing-service: PaymentConsumer routed via CoreBankingClient
                      → banking.payments.routed.v1
                        → state-service: PaymentRoutedConsumer → DB write SENT 
            
                POST /banking/webhook/payment-result (ingestion:8081)
                  → banking.payment.events.v1 (Kafka)
                    → state-service: PaymentEventConsumer → DB transition SENT→CONFIRMED 
                      → coreBankingRef: "FEDWIRE-001" persisted 
         ```
            # Submit a WIRE payment
            PAYMENT_ID=$(curl -s -X POST localhost:8081/api/v1/banking/payments \
              -H "Content-Type: application/json" \
              -H "Idempotency-Key: wire-002" \
              -d '{"userId":"user-42","amount":5000,"currency":"USD","type":"WIRE","region":"US"}' | jq -r '.paymentId')

            echo "Payment ID: $PAYMENT_ID"
            Payment ID: 04c16d44-d92d-4681-b112-ddd37b3d7c25
            
            # Wait 3s, Check state  (should be SENT)
            sleep 3 && curl -s localhost:8083/api/v1/banking/payments/$PAYMENT_ID | jq .
            {
              "id": 2,
              "paymentId": "04c16d44-d92d-4681-b112-ddd37b3d7c25",
              "idempotencyKey": "wire-002",
              "userId": "user-42",
              "amount": 5000,
              "currency": "USD",
              "type": "WIRE",
              "region": "US",
              "status": "SENT",
              "coreBankingRef": null,
              "createdAt": "2026-06-15T15:54:01.562335Z",
              "updatedAt": "2026-06-15T15:54:01.562336Z"
            }

            # Simulate core banking webhook (SUCCESS)
            curl -s -X POST localhost:8081/api/v1/banking/webhook/payment-result \
              -H "Content-Type: application/json" \
              -d "{\"paymentId\":\"$PAYMENT_ID\",\"coreBankingRef\":\"FEDWIRE-001\",\"result\":\"SUCCESS\",\"reason\":\"\"}" | jq .

            # Verify CONFIRMED
            sleep 2 && curl -s localhost:8083/api/v1/banking/payments/$PAYMENT_ID | jq .
            {
              "id": 2,
              "paymentId": "04c16d44-d92d-4681-b112-ddd37b3d7c25",
              "idempotencyKey": "wire-002",
              "userId": "user-42",
              "amount": 5000,
              "currency": "USD",
              "type": "WIRE",
              "region": "US",
              "status": "CONFIRMED",
              "coreBankingRef": "FEDWIRE-001",
              "createdAt": "2026-06-15T15:54:01.562335Z",
              "updatedAt": "2026-06-15T15:54:05.056173Z"
            }
        
            # Check payment stats
            curl -s localhost:8083/api/v1/banking/payments/stats | jq .
            {
              "failed": 0,
              "confirmed": 1,
              "sent": 1,
              "pending": 0
            }

            # Check audit files
            find /tmp/audit-lake -type f | head -10
            /tmp/audit-lake/year=2026/month=06/day=15/event_type=PENDING/0312dc15-c996-4394-9c47-6f90d325a4e5.json
            /tmp/audit-lake/year=2026/month=06/day=15/event_type=PENDING/53491a3e-1006-45c1-8aba-59da9362f169.json

            cat /tmp/audit-lake/year=2026/month=06/day=15/event_type=PENDING/*.json | head -1 | jq .
            {
              "eventId": "0312dc15-c996-4394-9c47-6f90d325a4e5",
              "paymentId": "71bec89f-5911-4c47-afa2-74c604b731e4",
              "userId": "user-42",
              "amount": "100",
              "currency": "USD",
              "type": "ACH",
              "region": "US",
              "status": "PENDING",
              "coreBankingRef": "",
              "occurredAt": "2026-06-15T16:12:20.518077255Z"
            }
            {
              "eventId": "53491a3e-1006-45c1-8aba-59da9362f169",
              "paymentId": "6d4b05f1-2199-425b-b0f9-e4ae38572713",
              "userId": "user-42",
              "amount": "200",
              "currency": "USD",
              "type": "ACH",
              "region": "US",
              "status": "PENDING",
              "coreBankingRef": "",
              "occurredAt": "2026-06-15T16:13:10.906444470Z"
            }
            
            # Consume discrepancy events
            docker exec kafka kafka-console-consumer \
              --bootstrap-server kafka:29092 \
              --topic banking.recon.discrepancies.v1 \
              --from-beginning --max-messages 5
            "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"dc34e2c5-20cf-4d92-b237-677a4bc9a248\",\"userId\":\"user-42\",\"amount\":\"100.0000\",\"currency\":\"USD\",\"paymentType\":\"ACH\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T16:04:35.154837Z\",\"detectedAt\":\"2026-06-15T16:07:14.177807745Z\"}"
            "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"dc34e2c5-20cf-4d92-b237-677a4bc9a248\",\"userId\":\"user-42\",\"amount\":\"100.0000\",\"currency\":\"USD\",\"paymentType\":\"ACH\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T16:04:35.154837Z\",\"detectedAt\":\"2026-06-15T16:08:14.182858077Z\"}"
            "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"dc34e2c5-20cf-4d92-b237-677a4bc9a248\",\"userId\":\"user-42\",\"amount\":\"100.0000\",\"currency\":\"USD\",\"paymentType\":\"ACH\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T16:04:35.154837Z\",\"detectedAt\":\"2026-06-15T16:09:14.187792059Z\"}"
            "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"dc34e2c5-20cf-4d92-b237-677a4bc9a248\",\"userId\":\"user-42\",\"amount\":\"100.0000\",\"currency\":\"USD\",\"paymentType\":\"ACH\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T16:04:35.154837Z\",\"detectedAt\":\"2026-06-15T16:10:14.194509982Z\"}"
            "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"dc34e2c5-20cf-4d92-b237-677a4bc9a248\",\"userId\":\"user-42\",\"amount\":\"100.0000\",\"currency\":\"USD\",\"paymentType\":\"ACH\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T16:04:35.154837Z\",\"detectedAt\":\"2026-06-15T16:11:48.382337570Z\"}"
            Processed a total of 5 messages

            # Idempotency proof (replay wire-001 → 409)
            curl -s -X POST localhost:8081/api/v1/banking/payments \
              -H "Content-Type: application/json" \
              -H "Idempotency-Key: wire-001" \
              -d '{"userId":"user-42","amount":5000,"currency":"USD","type":"WIRE","region":"US"}' | jq .
            {
              "paymentId": null,
              "status": "DUPLICATE",
              "message": "Idempotency-Key already processed"
            }  
              
            # Payement History:
            curl -s localhost:8083/api/v1/banking/payments/user/user-42 | jq '[.[] | {paymentId, type, status, coreBankingRef}]'
            [
              {
                "paymentId": "04c16d44-d92d-4681-b112-ddd37b3d7c25",
                "type": "WIRE",
                "status": "CONFIRMED",
                "coreBankingRef": "FEDWIRE-001"
              },
              {
                "paymentId": "c263d433-0755-445f-9ae7-4e83c5029818",
                "type": "WIRE",
                "status": "SENT",
                "coreBankingRef": null
              }
            ]
  
            # EOD reconciliation proof 
            # Submit a payment and DON'T send webhook (leaves it stuck in SENT)
            STUCK_ID=$(curl -s -X POST localhost:8081/api/v1/banking/payments \
              -H "Content-Type: application/json" \
              -H "Idempotency-Key: wire-stuck-002" \
              -d '{"userId":"user-42","amount":999,"currency":"USD","type":"WIRE","region":"US"}' | jq -r '.paymentId')
            echo "Stuck payment: $STUCK_ID"
            
            Stuck payment: 117af706-497c-4bdb-9e51-83c4468445a4
            
            # Wait 2 minutes (SLA = 120s), then check recon topic
            docker exec kafka kafka-console-consumer \
              --bootstrap-server kafka:29092 \
              --topic banking.recon.discrepancies.v1 \
              --from-beginning --max-messages 5
            "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"c263d433-0755-445f-9ae7-4e83c5029818\",\"userId\":\"user-42\",\"amount\":\"5000.0000\",\"currency\":\"USD\",\"paymentType\":\"WIRE\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T15:53:00.922731Z\",\"detectedAt\":\"2026-06-15T15:55:14.017816322Z\"}"
        "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"c263d433-0755-445f-9ae7-4e83c5029818\",\"userId\":\"user-42\",\"amount\":\"5000.0000\",\"currency\":\"USD\",\"paymentType\":\"WIRE\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T15:53:00.922731Z\",\"detectedAt\":\"2026-06-15T15:56:14.091349140Z\"}"
        "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"c263d433-0755-445f-9ae7-4e83c5029818\",\"userId\":\"user-42\",\"amount\":\"5000.0000\",\"currency\":\"USD\",\"paymentType\":\"WIRE\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T15:53:00.922731Z\",\"detectedAt\":\"2026-06-15T15:57:14.111760965Z\"}"
        "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"c263d433-0755-445f-9ae7-4e83c5029818\",\"userId\":\"user-42\",\"amount\":\"5000.0000\",\"currency\":\"USD\",\"paymentType\":\"WIRE\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T15:53:00.922731Z\",\"detectedAt\":\"2026-06-15T15:58:14.119873493Z\"}"
        "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"c263d433-0755-445f-9ae7-4e83c5029818\",\"userId\":\"user-42\",\"amount\":\"5000.0000\",\"currency\":\"USD\",\"paymentType\":\"WIRE\",\"region\":\"US\",\"stuckSince\":\"2026-06-15T15:53:00.922731Z\",\"detectedAt\":\"2026-
         ```       
 
