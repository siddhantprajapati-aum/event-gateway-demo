package com.iam.mq.event.gateway.adapter;


import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

@Component
public class LedgerRecipientAdapter implements RecipientAdapter {
    @Override
    public String recipientCode() {
        return "LEDGER_SVC";
    }

    @Override
    public MediaType contentType() {
        return MediaType.APPLICATION_JSON;
    }

    @Override
    public String transformPayload(String rawXml) {
        // Transform XML to downstream JSON schema
        return "{\"source\":\"gateway\",\"rawLength\":" + rawXml.length() + "}";
    }
}
