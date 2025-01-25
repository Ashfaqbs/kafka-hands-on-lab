package com.example.sb_kafka_docker_demo.controller;


import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.sb_kafka_docker_demo.kafka.KafkaProducerService;

@RestController
@RequestMapping("/api")
public class KafkaController {

    private final KafkaProducerService producerService;

    public KafkaController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping("/send")
    public String sendMessage() {
        producerService.sendMessage();
        return "Message sent";
    }
}
