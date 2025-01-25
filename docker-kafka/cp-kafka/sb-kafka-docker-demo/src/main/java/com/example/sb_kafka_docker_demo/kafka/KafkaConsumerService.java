package com.example.sb_kafka_docker_demo.kafka;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.example.sb_kafka_docker_demo.dto.MyMessage;

@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "my-topic", groupId = "test-group")
public void listen(MyMessage message) {
    System.out.println("Received message: " + message.getKey() + " - " + message.getValue());
}
/*
Received message: testKey - testValue
Received message: testKey - testValue
Received message: testKey - testValue

 */





//String Values Working fine
//  To test
//  when need to use change the config in prop file , and chage the Kafkatemplate value to string in KafkaProducerService.java and send Object.toString or a some string value

// @KafkaListener(topics = "my-topic", groupId = "test-group")
// public void listenString(String message) {
//     System.out.println("Received message: " + message);
// }
/*
Received message: com.example.sb_kafka_docker_demo.dto.MyMessage@e52a563
Received message: com.example.sb_kafka_docker_demo.dto.MyMessage@6a36d129
Received message: com.example.sb_kafka_docker_demo.dto.MyMessage@ecefe78
Received message: com.example.sb_kafka_docker_demo.dto.MyMessage@5c11955e
 
*/

}
