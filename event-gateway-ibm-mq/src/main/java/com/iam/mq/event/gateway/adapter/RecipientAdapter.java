package com.iam.mq.event.gateway.adapter;

import org.springframework.http.MediaType;

import java.awt.*;

public interface RecipientAdapter {
    String recipientCode();
    String transformPayload(String rawXml);
    MediaType contentType();
}
