package com.example.sb_kafka_docker_demo.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
@Configuration

public class Kafkaconfig {
    


    @Bean 
public NewTopic topic1() 
{ 
    return TopicBuilder.name("my-topic") 
        .partitions(1) 
        .replicas(1) 
        .build(); 
} 
}
