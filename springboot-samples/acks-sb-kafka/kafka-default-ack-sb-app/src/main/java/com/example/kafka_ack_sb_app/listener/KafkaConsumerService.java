package com.example.kafka_ack_sb_app.listener;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {


    
    /*
     * 
     *  Default ack mode 
     * 
     * 
      */
    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void consume(String message) {
        System.out.println("Message received: " + message);
    }


    /* 

curl http://localhost:8080/api/send
Message sent: Hello Kafka

Message sent: Hello Kafka
Message received: Hello Kafka


     */

/*
 In Spring Boot's integration with Apache Kafka, the default acknowledgment configuration is set to AckMode.BATCH. 
 This means that the consumer commits(acknowledge) offsets after processing a batch of messages, rather than after each individual message. 
 This behavior is controlled by the enable.auto.commit property, which is set to false by default in Spring Kafka, allowing the container to manage offset commits.
 

batching refers to the process of grouping multiple records together to send them to the broker in a single request. 

This approach enhances throughput and reduces the number of requests, thereby improving performance.

Producer-Side Batching:

batch.size: This configuration parameter determines the maximum size (in bytes) of a batch that the producer will attempt to send. 
The default value is 16,384 bytes (16 KB). If the accumulated records for a partition reach this size, the batch is sent immediately. 
It's important to note that this size is an upper limit; if the batch doesn't reach this size, it can still be sent based on other criteria. 

linger.ms: This setting specifies the amount of time the producer will wait for additional records before sending a batch. By default, it's set to 0 milliseconds, meaning the producer sends records as soon as they are available. 
Increasing this value allows the producer to wait for more records, potentially increasing batch sizes and improving throughput. 


Consumer-Side Batching in Spring Boot:

In Spring Boot applications utilizing Spring Kafka, the consumer can process messages in batches. The size of these batches is influenced by several Kafka consumer configurations:

max.poll.records: This parameter defines the maximum number of records that a consumer will fetch in a single poll operation. 
The default value is 500. Adjusting this value allows the consumer to retrieve more or fewer records per poll, effectively controlling the batch size.

fetch.min.bytes: This setting indicates the minimum amount of data (in bytes) that the broker should return for a fetch request. 
If the available data is less than this value, the broker will wait until more data is available or until fetch.max.wait.ms is reached.

fetch.max.wait.ms: This parameter specifies the maximum amount of time the broker will wait before returning data, even if the fetch.min.bytes threshold isn't met.

By configuring these parameters, we can control the size and timing of the batches your consumer processes. For instance, increasing max.poll.records allows the consumer to retrieve and process larger batches, which can be more efficient under certain workloads. 
 
 
 */

    
    
    
    /*
 //  * 
    //  *  Ack mode set to MANUAL
    //  *  Kafka Consumer that logs and acknowledges each message
    //  * 
    //  */

    //    @KafkaListener(topics = "my-topic", groupId = "my-group")
    //     public void consume(String message, Acknowledgment ack) {
    //         System.out.println("Received Message: " + message);
    //         // Acknowledge the message after processing
    //         ack.acknowledge();
    //         System.out.println("Acknowledged Message: " + message);

    //     }





}