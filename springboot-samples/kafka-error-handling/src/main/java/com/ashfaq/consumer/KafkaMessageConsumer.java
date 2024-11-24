package com.ashfaq.consumer;

import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import com.ashfaq.dto.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaMessageConsumer {
	
//	Use Case
//	The @RetryableTopic annotation is useful 
//	when you expect that transient errors might be resolved by simply retrying 
//	the operation after a short wait. This is common in scenarios like network issues, 
//	temporary unavailability of external services, or temporary database connectivity issues.

	// here we are stimulating the issue by throwing error , so here we will add
	// retry if retry exceeds then send to DLT topic

	
	// 4 means it will try 3 times , and default count is 3
	//delay explained 1st retry after 3 secs, 2nd retry after 6.75 secs, 3rd retry after 10.125 secs because of multiplier 6.75 seconds * 1.5
	//If any retry delay calculation exceeds 15 seconds, the delay will be capped at 15 seconds due to the maxDelay parameter.
//	Also We can sprecify for what type of exception we can retry  and dont retry 
	
//	NOTE : error can be seen Caused by: java.lang.IllegalArgumentException: Please use only retryOn() or only notRetryOn() 
//	when we use both include and exclude so use one 
	@RetryableTopic(attempts = "4",backoff = @Backoff(delay = 3000, multiplier = 1.5 ,maxDelay = 15000) ,
			include = {RuntimeException.class, UnknownHostException.class} 
//	, 
//			exclude = {NullPointerException.class}
	)
	@KafkaListener(topics = "${app.topic.name}", groupId = "my-group")
	public void consumeEvents(User user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset) {

		try {
			log.info("Received: {} from {} offset {}", new ObjectMapper().writeValueAsString(user), topic, offset);
			// validate restricted IP before process the records
			List<String> restrictedIpList = Stream.of("126.130.43.183").collect(Collectors.toList());
			if (restrictedIpList.contains(user.getIpAddress())) {
				throw new RuntimeException("Invalid IP Address received !");
			}

		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

	}

	// Dead letter topic DLT , data will sent to this topic if the retry exceeds
	@DltHandler
	public void listenDLT(User user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset) {

//		a topic with dltname will be crearted as well , you can check in logs
		log.info("DLT recieved : {} , from {} , offset {}  " + user, topic, offset);
	}

}
