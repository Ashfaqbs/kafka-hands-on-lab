/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */



package com.kafkaDemo.SpringbootXkafkaDEMO;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

	@Autowired
	private KafkaStringProducer kafkaStringProducer;
	
	@GetMapping("/publishStringData")
	public String conumserAPI(@RequestParam("message")String data)
	{
		
		kafkaStringProducer.stringPublish(data);
		
		return "Successfully Sent";
	}
	@GetMapping("/publishData")
	public String conumserAPIFromoldTopic(@RequestParam("message")String data)
	{
		
		kafkaStringProducer.stringPublishfromOLDTopic(data);
		
		return "Successfully Sent";
	}
	
}
