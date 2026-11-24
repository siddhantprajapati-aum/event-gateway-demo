package com.iam.mq.event.gateway.service;

import com.iam.mq.event.gateway.adapter.RecipientAdapter;
import com.iam.mq.event.gateway.dto.DeliveryTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClient;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class WebhookDeliveryWorker {
    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryWorker.class);
    private final JdbcTemplate jdbc;
    private final RestClient restClient;
    private final Map<String, RecipientAdapter> adapters;
    private final int maxRetries;

    public WebhookDeliveryWorker(JdbcTemplate jdbc, List<RecipientAdapter> adapterList,
                                 @Value("${gateway.max-retries}") int maxRetries) {
        this.jdbc = jdbc;
        this.restClient = RestClient.create();
        this.adapters = adapterList.stream().collect(Collectors.toMap(RecipientAdapter::recipientCode, a -> a));
        this.maxRetries = maxRetries;
    }

    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void processBatch() throws SQLException {
        List<DeliveryTask> tasks = jdbc.query("""
    SELECT d.delivery_id, d.event_id, d.recipient_code, d.target_url, d.retry_count, r.raw_xml
    FROM delivery_queue d
    JOIN raw_events r ON d.event_id = r.event_id
    WHERE d.status = 'PENDING' AND d.next_attempt_at <= SYSTIMESTAMP AND ROWNUM <= 10
    FOR UPDATE OF d.status SKIP LOCKED
    """,  (rs, rowNum) -> new DeliveryTask(
                rs.getLong("delivery_id"),
                rs.getString("event_id"),
                rs.getString("recipient_code"),
                rs.getString("target_url"),
                rs.getInt("retry_count"),
                rs.getString("raw_xml")
        ));

        for (DeliveryTask task : tasks) {
            MDC.put("correlationId", task.eventId());
            try {
                RecipientAdapter adapter = adapters.get(task.recipientCode());
                String payload = (adapter != null) ? adapter.transformPayload(task.rawXml()) : task.rawXml();
                MediaType type = (adapter != null) ? adapter.contentType() : MediaType.APPLICATION_XML;

                restClient.post()
                        .uri(task.targetUrl())
                        .contentType(type)
                        .header("X-Correlation-Id", task.eventId())
                        .body(payload)
                        .retrieve()
                        .toBodilessEntity();

                jdbc.update("UPDATE delivery_queue SET status = 'DELIVERED', updated_at = SYSTIMESTAMP WHERE delivery_id = ?",
                        task.deliveryId());
                log.info("Webhook delivered to {}", task.recipientCode());
            } catch (Exception ex) {
                handleFailure(task, ex.getMessage());
            } finally {
                MDC.clear();
            }
        }
    }

    private void handleFailure(DeliveryTask task, String error) throws SQLException {
        int nextRetry = task.retryCount() + 1;
        if (nextRetry >= maxRetries) {
            jdbc.update("""
                UPDATE delivery_queue
                SET status = 'DEAD_LETTER', retry_count = ?, last_error = ?, updated_at = SYSTIMESTAMP
                WHERE delivery_id = ?
                """, nextRetry, truncate(error), task.deliveryId());
            log.error("Delivery moved to DEAD_LETTER after {} attempts", nextRetry);
        } else {
            long backoffSeconds = (long) Math.pow(2, nextRetry) * 10; // 20s, 40s, 80s...
            jdbc.update("""
                UPDATE delivery_queue
                SET retry_count = ?, next_attempt_at = SYSTIMESTAMP + NUMTODSINTERVAL(?, 'SECOND'),
                    last_error = ?, updated_at = SYSTIMESTAMP
                WHERE delivery_id = ?
                """, nextRetry, backoffSeconds, truncate(error), task.deliveryId());
            log.warn("Delivery failed; retry {} scheduled in {}s", nextRetry, backoffSeconds);
        }
    }

    private String truncate(String s) {
        return s == null ? "" : s.substring(0, Math.min(s.length(), 950));
    }
}