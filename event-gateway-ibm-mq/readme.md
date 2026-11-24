# Event Gateway Demo (`event-gateway-demo`)

A stateless, enterprise-grade Spring Boot event gateway that consumes XML events from **IBM MQ**, validates them against versioned **XSD schemas**, persists raw and relational data into **Oracle Database** transactionally, and reliably delivers transformed payloads to downstream **webhooks** using a concurrency-safe database queue (`FOR UPDATE SKIP LOCKED`) with retries and dead-letter handling.

---

## 1. Technologies Used

| Category | Technology | Purpose |
| :--- | :--- | :--- |
| **Core Framework** | Java 21, Spring Boot 4.1.2 | Application runtime, DI, scheduling, and HTTP `RestClient` |
| **Enterprise Messaging** | IBM MQ (`mq-jms-spring-boot-starter` 4.1.2) | Source message broker for incoming XML events (`DEV.QUEUE.1`) |
| **Database** | Oracle Database Free 23 (`ojdbc11`), Spring JDBC | Raw XML storage, expanded relational tables, and DB-backed delivery queue |
| **DB Migrations** | Flyway (`spring-boot-starter-flyway`, `flyway-database-oracle`) | Automated Oracle schema migrations (`V1__init_gateway.sql`) |
| **XML Processing** | Jakarta XML Binding (JAXB) & W3C XSD | Schema validation and XML-to-Java unmarshalling |
| **Outbound Webhooks** | Spring `RestClient` | Reliable HTTP POST delivery with retries and dead-lettering |
| **Observability** | Spring Boot Structured Logging (ECS JSON), SLF4J `MDC`, Actuator | End-to-end correlation ID tracing across MQ, DB, and webhooks |
| **Local Infra** | Docker & Docker Compose | Running local IBM MQ and Oracle Database containers |
| **Deployment (Optional)** | WinSW (`event-gateway.xml`) | Running the Spring Boot JAR natively as a stateless Windows Service |

---

## 2. Project Structure & File Roles

```text
event-gateway-demo/
├── docker-compose.yml                         # Spins up local IBM MQ and Oracle Free 23 containers
├── event-gateway.xml                          # WinSW configuration for Windows Service deployment
├── pom.xml                                    # Maven dependencies (Spring Boot 4, IBM MQ, Oracle, Flyway, JAXB)
└── src/main/
    ├── java/com/iam/mq/event/gateway/
    │   ├── EventGatewayDemoApplication.java   # Main entry point (annotated with @EnableScheduling)
    │   ├── adapter/
    │   │   ├── RecipientAdapter.java          # Extensible interface for downstream payload transformations
    │   │   └── LedgerRecipientAdapter.java    # Sample adapter transforming XML to JSON for LEDGER_SVC
    │   ├── config/
    │   │   ├── GatewayProperties.java         # Binds gateway.* routing and retry config from application.yaml
    │   │   └── MqEventListener.java           # @JmsListener consuming IBM MQ, validating XML, and persisting to Oracle
    │   ├── dto/
    │   │   ├── DeliveryTask.java              # Java record representing a row polled from DELIVERY_QUEUE
    │   │   └── PaymentEventXml.java           # JAXB-annotated DTO mapped to the PaymentEvent XML schema
    │   └── service/
    │       ├── XmlEventParser.java            # Validates raw XML against XSD and unmarshals into PaymentEventXml
    │       └── WebhookDeliveryWorker.java     # @Scheduled worker polling Oracle with SKIP LOCKED and sending webhooks
    └── resources/
        ├── application.yaml                   # Oracle, IBM MQ, ECS logging, and event-to-webhook routing config
        ├── db/migration/
        │   └── V1__init_gateway.sql           # Flyway script creating RAW_EVENTS, PAYMENT_EVENTS, and DELIVERY_QUEUE
        └── xsd/
            └── payment-event-v1.xsd           # Versioned XSD schema for validating incoming PaymentEvent XML
```

---

## 3. End-to-End XML Event Flow

```text
IBM MQ (DEV.QUEUE.1)
   │
   ▼  1. Consumed concurrently (@JmsListener, concurrency = "2-4")
MqEventListener
   │
   ├─► 2. XmlEventParser validates XML against payment-event-v1.xsd & unmarshals via JAXB
   ├─► 3. Sets MDC "correlationId" = eventId for structured JSON logging
   │
   ▼  4. Single @Transactional Oracle write:
   ├──► RAW_EVENTS      (Idempotency check via event_id; duplicates are ignored)
   ├──► PAYMENT_EVENTS  (Expanded relational columns: account_no, amount, currency)
   └──► DELIVERY_QUEUE  (One PENDING row per configured downstream recipient)
   │
   ▼  5. Polled every 2s by WebhookDeliveryWorker (@Scheduled)
   │     SELECT ... FOR UPDATE OF d.status SKIP LOCKED (safe across multiple nodes)
   │
   ├─► 6. Looks up RecipientAdapter (e.g., LedgerRecipientAdapter transforms XML → JSON)
   ├─► 7. Sends HTTP POST via RestClient with X-Correlation-Id header
   │
   ▼  8. Updates DELIVERY_QUEUE status:
   ├──► Success       → status = 'DELIVERED'
   ├──► Failure < max → retry_count++, exponential backoff (20s, 40s, 80s...)
   └──► Max retries   → status = 'DEAD_LETTER', stores error in last_error
```

---

## 4. Webhook Architecture & Features

### Configurable Routing
Event-to-recipient routing is driven entirely by `application.yaml`. A single XML event can fan out to multiple downstream webhooks:
```yaml
gateway:
  mq-queue: DEV.QUEUE.1
  max-retries: 3
  routes:
    PAYMENT_CREATED:
      - recipientCode: LEDGER_SVC
        targetUrl: [https://webhook.site/](https://webhook.site/)<your-unique-id>
      - recipientCode: ALERT_SVC
        targetUrl: [https://webhook.site/](https://webhook.site/)<your-unique-id>
```

### Extensible Payload Transformation (`RecipientAdapter`)
- Before sending the HTTP `POST`, `WebhookDeliveryWorker` looks up a Spring bean implementing `RecipientAdapter` matching the `recipientCode`.
- **With an adapter** (e.g., `LedgerRecipientAdapter` for `LEDGER_SVC`): transforms the raw XML into a custom format (such as `application/json`).
- **Without an adapter** (e.g., `ALERT_SVC`): falls back to sending the original raw XML with `application/xml`.
- **Adding a new recipient:** Implement `RecipientAdapter` as a `@Component` and add the route in `application.yaml`—no changes to core gateway code required.

### Concurrency-Safe Multi-Node Polling
- `WebhookDeliveryWorker` polls `DELIVERY_QUEUE` using Oracle's `FOR UPDATE OF d.status SKIP LOCKED`.
- Multiple stateless instances (e.g., across multiple Windows Server nodes) can poll in parallel without blocking each other or double-delivering the same webhook.

### Retries, Backoff, & Dead-Letter Queue (DLQ)
- Every outbound webhook includes the `X-Correlation-Id: <eventId>` header for downstream tracing and idempotency.
- If a downstream endpoint returns a 4xx/5xx error or times out:
    - `retry_count` is incremented and `next_attempt_at` is pushed forward using exponential backoff (`2^retry * 10` seconds: 20s, 40s, 80s...).
    - The error response is saved in `DELIVERY_QUEUE.last_error`.
    - Once `retry_count` reaches `gateway.max-retries`, the row is marked **`DEAD_LETTER`** so it stops blocking the queue.

### Replaying `DEAD_LETTER` Webhooks
Once a downstream endpoint is fixed (or its URL is updated), replay failed deliveries directly in Oracle:
```sql
UPDATE delivery_queue
SET status = 'PENDING',
    retry_count = 0,
    next_attempt_at = SYSTIMESTAMP,
    last_error = NULL
WHERE status = 'DEAD_LETTER';

COMMIT;
```

---

## 5. How to Run the Code

### Prerequisites
- **Java 21** & **Maven**
- **Docker Desktop** (running)
- A unique test webhook URL from [webhook.site](https://webhook.site)

### Step 1: Configure Webhook URLs
Open `src/main/resources/application.yaml` and replace the placeholder `targetUrl` values under `gateway.routes.PAYMENT_CREATED` with your real URL from [webhook.site](https://webhook.site).

### Step 2: Start IBM MQ and Oracle via Docker
```bash
docker compose up -d
```
Wait ~30–60 seconds for Oracle to initialize on first launch. Verify it is ready:
```bash
docker compose logs -f oracle
# Wait until you see: DATABASE IS READY TO USE!
```

### Step 3: Run the Spring Boot Application
Run `EventGatewayDemoApplication` from IntelliJ IDEA or via Maven:
```bash
./mvnw spring-boot:run
```
On startup, Flyway automatically executes `V1__init_gateway.sql` to create the Oracle tables.

---

## 6. How to Send an Event & Check Data Delivery

### Step 1: Push a Test XML Message to IBM MQ
1. Open the IBM MQ Web Console in an Incognito window: **`https://localhost:9443/ibmmq/console/`**
2. Log in with **`admin`** / **`passw0rd`**.
3. Go to **Manage** → **QM1** → **Queues** → **`DEV.QUEUE.1`** → **Create message**.
4. Keep Format as **`MQSTR`**, paste the XML below into **Application data**, and click **Create**:

```xml
<PaymentEvent xmlns="http://gateway.example.com/events/v1" version="1.0">
  <eventId>EVT-1003</eventId>
  <eventType>PAYMENT_CREATED</eventType>
  <accountNo>ACC-9876543210</accountNo>
  <amount>15000.50</amount>
  <currency>INR</currency>
</PaymentEvent>
```

### Step 2: Verify Delivery
- **Application Console Logs (ECS JSON):** Look for these three log lines sharing `"correlationId":"EVT-1002"`:
    1. `"Event persisted and queued for delivery"`
    2. `"Webhook delivered to LEDGER_SVC"`
    3. `"Webhook delivered to ALERT_SVC"`
- **Webhook.site Dashboard:** You will see two incoming `POST` requests—one JSON payload (transformed by `LedgerRecipientAdapter`) and one raw XML payload (for `ALERT_SVC`), both carrying the `X-Correlation-Id: EVT-1002` header.
- **Test Idempotency:** Send the exact same `EVT-1002` XML again—the app will log `"Duplicate event ignored"` without creating duplicate DB rows or webhooks.
- **Test Retries & DLQ:** Point a route to an invalid URL (e.g., `https://webhook.site/invalid-id`), send a new `eventId`, and watch the logs retry 3 times before moving the row to `DEAD_LETTER` with the HTTP 404 details stored in `last_error`.

---

## 7. How to View Data in Oracle Database

### Option A: Terminal (SQL*Plus via Docker)
```bash
docker compose exec oracle sqlplus gateway/gateway@//localhost:1521/FREEPDB1
```

Run these queries at the `SQL>` prompt:

```sql
SET LINESIZE 200
COL event_id FORMAT A12
COL recipient_code FORMAT A15
COL status FORMAT A12
COL account_no FORMAT A18
COL last_error FORMAT A60

-- 1. Check webhook delivery statuses & any failure reasons
SELECT delivery_id, event_id, recipient_code, status, retry_count, last_error
FROM delivery_queue;

-- 2. Check expanded relational payment data
SELECT event_id, account_no, amount, currency
FROM payment_events;

-- 3. Check raw XML audit log
SELECT event_id, event_type, schema_version, received_at
FROM raw_events;
```

Type `EXIT` to leave SQL*Plus.

### Option B: Database GUI (DBeaver / Oracle SQL Developer / IntelliJ)
- **Host:** `localhost`
- **Port:** `1521`
- **Service Name:** `FREEPDB1` *(use Service Name, not SID)*
- **Username / Password:** `gateway` / `gateway`