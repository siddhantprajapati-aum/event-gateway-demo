package com.iam.mq.event.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private String mqQueue;
    private int maxRetries = 3;
    private Map<String, List<RouteConfig>> routes = new HashMap<>();

    public List<RouteConfig> getRoutesFor(String eventType) {
        return routes.getOrDefault(eventType, Collections.emptyList());
    }

    public String getMqQueue() { return mqQueue; }
    public void setMqQueue(String mqQueue) { this.mqQueue = mqQueue; }

    public int getMaxRetries() { return maxRetries; }
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }

    public Map<String, List<RouteConfig>> getRoutes() { return routes; }
    public void setRoutes(Map<String, List<RouteConfig>> routes) { this.routes = routes; }

    public record RouteConfig(String recipientCode, String targetUrl) {}
}
