package com.iam.mq.event.gateway.service;

import com.iam.mq.event.gateway.dto.PaymentEventXml;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Unmarshaller;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.StringReader;

@Component
public class XmlEventParser {

    private final JAXBContext jaxbContext;
    private final Schema schema;

    public XmlEventParser() {
        try {
            this.jaxbContext = JAXBContext.newInstance(PaymentEventXml.class);
            SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            this.schema = factory.newSchema(new ClassPathResource("xsd/payment-event-v1.xsd").getURL());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize XML parser / XSD schema", e);
        }
    }

    public PaymentEventXml validateAndParse(String rawXml) {
        try {
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            unmarshaller.setSchema(schema);
            return (PaymentEventXml) unmarshaller.unmarshal(new StringReader(rawXml));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid XML payload: " + e.getMessage(), e);
        }
    }
}