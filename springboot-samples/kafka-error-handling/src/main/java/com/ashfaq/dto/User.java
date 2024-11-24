package com.ashfaq.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
	private int id;
	private String firstName;
	private String lastName;
	private String email;
	private String gender;
	private String ipAddress;
}