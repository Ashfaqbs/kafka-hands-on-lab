package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KStreamsDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(KStreamsDemoApplication.class, args);
		System.out.println("Working directory: " + System.getProperty("user.dir"));

	}

}
