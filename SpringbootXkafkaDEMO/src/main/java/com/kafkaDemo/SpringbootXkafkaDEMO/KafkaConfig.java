/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.kafkaDemo.SpringbootXkafkaDEMO;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

	@Bean
	public NewTopic myCustomStringTopic() {

		return TopicBuilder.name("MyStringTopic1")
//				.partitions(9)
				.build();
	}

}
