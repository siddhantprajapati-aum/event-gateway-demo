package com.iam.mq.event.gateway.dto;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.math.BigDecimal;

@XmlRootElement(name = "PaymentEvent", namespace = "http://gateway.example.com/events/v1")
@XmlAccessorType(XmlAccessType.FIELD)
public class PaymentEventXml {

    @XmlAttribute(name = "version")
    private String version = "1.0";

    @XmlElement(namespace = "http://gateway.example.com/events/v1", required = true)
    private String eventId;

    @XmlElement(namespace = "http://gateway.example.com/events/v1", required = true)
    private String eventType;

    @XmlElement(namespace = "http://gateway.example.com/events/v1", required = true)
    private String accountNo;

    @XmlElement(namespace = "http://gateway.example.com/events/v1", required = true)
    private BigDecimal amount;

    @XmlElement(namespace = "http://gateway.example.com/events/v1", required = true)
    private String currency;

    public String getVersion() { return version; }
    public String getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public String getAccountNo() { return accountNo; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
}