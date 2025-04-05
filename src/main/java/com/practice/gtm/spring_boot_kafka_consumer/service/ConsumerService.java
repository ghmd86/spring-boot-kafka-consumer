package com.practice.gtm.spring_boot_kafka_consumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    @KafkaListener(topics = "location-event", groupId = "consumer-group")
    public void getLocation(String location) {
        System.out.println(location);
    }
}
