package com.iam.mq.event.gateway.dto;

public record DeliveryTask(
        Long deliveryId,
        String eventId,
        String recipientCode,
        String targetUrl,
        int retryCount,
        String rawXml
) {
}