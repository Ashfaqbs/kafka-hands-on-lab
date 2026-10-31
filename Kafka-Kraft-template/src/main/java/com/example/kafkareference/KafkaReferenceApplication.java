package com.example.kafkareference;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaReferenceApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaReferenceApplication.class, args);
    }
}
