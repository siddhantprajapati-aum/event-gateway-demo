CREATE TABLE raw_events (
                            event_id        VARCHAR2(64) PRIMARY KEY,
                            event_type      VARCHAR2(50) NOT NULL,
                            schema_version  VARCHAR2(20) NOT NULL,
                            raw_xml         CLOB NOT NULL,
                            received_at     TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE payment_events (
                                event_id        VARCHAR2(64) PRIMARY KEY REFERENCES raw_events(event_id),
                                account_no      VARCHAR2(34) NOT NULL,
                                amount          NUMBER(18, 2) NOT NULL,
                                currency        VARCHAR2(3) NOT NULL
);

CREATE TABLE delivery_queue (
                                delivery_id     NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                                event_id        VARCHAR2(64) NOT NULL REFERENCES raw_events(event_id),
                                recipient_code  VARCHAR2(50) NOT NULL,
                                target_url      VARCHAR2(500) NOT NULL,
                                status          VARCHAR2(20) DEFAULT 'PENDING' NOT NULL, -- PENDING, DELIVERED, DEAD_LETTER
                                retry_count     NUMBER DEFAULT 0 NOT NULL,
                                next_attempt_at TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL,
                                last_error      VARCHAR2(1000),
                                updated_at      TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX idx_delivery_poll ON delivery_queue (status, next_attempt_at);