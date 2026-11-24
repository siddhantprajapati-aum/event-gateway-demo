package com.iam.mq.event.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EventGatewayDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(EventGatewayDemoApplication.class, args);
	}

}
