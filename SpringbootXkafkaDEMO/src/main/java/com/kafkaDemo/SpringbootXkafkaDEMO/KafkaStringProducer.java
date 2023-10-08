/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.kafkaDemo.SpringbootXkafkaDEMO;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaStringProducer {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void stringPublish(String data) {
		kafkaTemplate.send("MyStringTopic1", data);

	}

	public void stringPublishfromOLDTopic(String data) {
		//
//	E:\kafka>.\bin\windows\kafka-topics.bat --create --topic myFirstTopic --bootstrap-server localhost:9092
//	Created topic myFirstTopic.
		
		 
		kafkaTemplate.send("myFirstTopic", data);

	}

}
