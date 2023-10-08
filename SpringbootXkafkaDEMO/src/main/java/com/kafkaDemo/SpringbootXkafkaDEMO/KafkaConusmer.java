/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.kafkaDemo.SpringbootXkafkaDEMO;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConusmer {

	@KafkaListener(topics = "MyStringTopic1", groupId = "myTutorialGroup")
	public void consumerString(String data) {
		System.out.println("String Data -> %s" + data);
	}
	@KafkaListener(topics = "myFirstTopic")
	public void consumerStringfromOLDTopic(String data) {
		System.out.println("String Data -> %s" + data);
	}

}
