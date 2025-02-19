package com.example.kafka_ack_sb_app.listener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
   
 /* 
  Ack mode set to MANUAL
  Kafka Consumer that logs and acknowledges each message
    
  */

    @KafkaListener(topics = "my-topic", groupId = "my-group")
    public void consume(String message, Acknowledgment acknowledgment) {
        try {
            // Process the message
            System.out.println("Message received in consumer: " + message);
            // Manually acknowledge the message
            acknowledgment.acknowledge();
            System.out.println(" Manual Message acknowledged: " + message);
        } catch (Exception e) {
            // Handle any exceptions during processing
            System.err.println("Error processing message: " + e.getMessage());
            // Optionally, we  can choose not to acknowledge the message to reprocess it later
        }
    }

/*


curl localhost:8080/api/send
Message sent: Hello Kafka

Message sent: Hello Kafka
Message received in consumer: Hello Kafka
 Manual Message acknowledged: Hello Kafka


 */



}