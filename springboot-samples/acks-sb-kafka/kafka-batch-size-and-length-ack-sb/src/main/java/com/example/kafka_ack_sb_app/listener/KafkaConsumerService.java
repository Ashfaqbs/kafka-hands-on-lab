package com.example.kafka_ack_sb_app.listener;


import java.nio.charset.StandardCharsets;
import java.util.List;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {



     private static final int MESSAGE_THRESHOLD = 25;

    // @KafkaListener(topics = "my-topic", groupId = "my-group", containerFactory = "batchFactory")
    public void consumebyCount(List<String> messages, Acknowledgment acknowledgment) {
        int batchCount = messages.size();
        System.out.println("Received batch with " + batchCount + " messages.");
        
        // Process each message (for example, log it)
        for (String message : messages) {
            System.out.println("Processing message: " + message);
        }

        // For this simulation, if the batch contains at least MESSAGE_THRESHOLD messages, we acknowledge.
        if (batchCount >= MESSAGE_THRESHOLD) {
            System.out.println("Acknowledging batch after processing " + batchCount + " messages.");
        } else {
            System.out.println("Batch has fewer than " + MESSAGE_THRESHOLD + " messages; acknowledging anyway to avoid re-delivery.");
        }
        acknowledgment.acknowledge();
    }


    
    
  
    /* 

 http://localhost:8080/api/send
Message sent: Hello Kafka
Received batch with 1 messages.
Processing message: Hello Kafka
Batch has fewer than 25 messages; acknowledging anyway to avoid re-delivery.


     */




     // Size threshold: 1 MB (1 * 1024 * 1024 bytes)
    private static final int SIZE_THRESHOLD = 1 * 1024 * 1024;

    @KafkaListener(topics = "my-topic", groupId = "my-group", containerFactory = "batchFactory")
    public void consumeBySize(List<String> messages, Acknowledgment acknowledgment) {
        int cumulativeSize = 0;
        for (String message : messages) {
            // Process the message (for example, log it)
            System.out.println("Processing message: " + message);
            cumulativeSize += message.getBytes(StandardCharsets.UTF_8).length;
        }

        System.out.println("Cumulative size of batch: " + cumulativeSize + " bytes.");

        if (cumulativeSize >= SIZE_THRESHOLD) {
            System.out.println("Acknowledging batch after reaching size threshold of " + SIZE_THRESHOLD + " bytes.");
        } else {
            System.out.println("Cumulative size below threshold; acknowledging batch to avoid re-delivery.");
        }
        acknowledgment.acknowledge();
    }



/*

http://localhost:8080/api/send

Message sent: Hello Kafka
Processing message: Hello Kafka
Cumulative size of batch: 11 bytes.
Cumulative size below threshold; acknowledging batch to avoid re-delivery.

*/













    }