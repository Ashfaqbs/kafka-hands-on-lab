package com.example.kafka_producer_sb3;

import com.example.kafka_producer_sb3.dto.EmployeeDTO;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
@RestController
@RequestMapping("/producer")
public class EmployeeProducerController {
    private final KafkaTemplate<String, EmployeeDTO> kafkaTemplate;

    public EmployeeProducerController(KafkaTemplate<String, EmployeeDTO> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendEmployee(@RequestBody EmployeeDTO employeeDTO) {
        System.out.println("Producer : " + employeeDTO);

        kafkaTemplate.send("employee-topic", employeeDTO);
        return ResponseEntity.ok("Employee sent to Kafka");
    }
}
