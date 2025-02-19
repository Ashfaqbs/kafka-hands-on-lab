package com.example.kafka_ack_sb_app.controller;



import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka_ack_sb_app.producer.KafkaProducerService;

@RestController
@RequestMapping("/api")
public class KafkaController {

    private final KafkaProducerService producerService;

    public KafkaController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping("/send")
    public String sendMessage(@RequestParam(defaultValue = "Hello Kafka") String message) {
        producerService.sendMessage("my-topic", message);
        return "Message sent: " + message;
    }
}