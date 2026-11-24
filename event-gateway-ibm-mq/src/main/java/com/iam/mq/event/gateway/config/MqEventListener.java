package com.iam.mq.event.gateway.config;

import com.iam.mq.event.gateway.dto.PaymentEventXml;
import com.iam.mq.event.gateway.service.XmlEventParser;

import org.springframework.dao.DuplicateKeyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;

@Component
public class MqEventListener {
    private static final Logger log = LoggerFactory.getLogger(MqEventListener.class);
    private final JdbcTemplate jdbc;
    private final XmlEventParser xmlParser;
    private final GatewayProperties props;

    public MqEventListener(JdbcTemplate jdbc, XmlEventParser xmlParser, GatewayProperties props) {
        this.jdbc = jdbc;
        this.xmlParser = xmlParser;
        this.props = props;
    }



    @JmsListener(destination = "${gateway.mq-queue}", concurrency = "2-4")
    @Transactional
    public void onMessage(String rawXml) {
        PaymentEventXml event = xmlParser.validateAndParse(rawXml);
        MDC.put("correlationId", event.getEventId());

        try {
            int inserted = jdbc.update("""
            INSERT INTO raw_events (event_id, event_type, schema_version, raw_xml)
            SELECT ?, ?, ?, ? FROM dual
            WHERE NOT EXISTS (SELECT 1 FROM raw_events WHERE event_id = ?)
            """, event.getEventId(), event.getEventType(), event.getVersion(), rawXml, event.getEventId());

            if (inserted == 0) {
                log.warn("Duplicate event ignored");
                return; // Returns normally -> JMS ACKs and removes message from MQ
            }

            jdbc.update("""
            INSERT INTO payment_events (event_id, account_no, amount, currency)
            VALUES (?, ?, ?, ?)
            """, event.getEventId(), event.getAccountNo(), event.getAmount(), event.getCurrency());

            for (var route : props.getRoutesFor(event.getEventType())) {
                jdbc.update("""
                INSERT INTO delivery_queue (event_id, recipient_code, target_url)
                VALUES (?, ?, ?)
                """, event.getEventId(), route.recipientCode(), route.targetUrl());
            }
            log.info("Event persisted and queued for delivery");

        } catch (DuplicateKeyException ex) {
            // Handles race condition where two threads insert the same eventId simultaneously
            log.warn("Duplicate event caught via PK constraint, ignoring");
            return; // Returns normally -> stops MQ redelivery loop
        } finally {
            MDC.clear();
        }
    }
}