/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */



package com.main.springboot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

	
	//String 
	@Bean
	public NewTopic myCustomTopic()
	{
		
		
		return TopicBuilder.name("myCustomTopic1")
//				.partitions(10) if not give it will take the default
				.build();
	}
	
	
	//For JSON
	@Bean
	public NewTopic myCustomJSONTopic()
	{
		
		
		return TopicBuilder.name("myCustomTopic1JSON")
//				.partitions(10) if not give it will take the default
				.build();
	}
	
}
