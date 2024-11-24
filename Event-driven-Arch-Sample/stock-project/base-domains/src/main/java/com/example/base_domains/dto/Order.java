package com.example.base_domains.dto;

import lombok.Data;

@Data
public class Order {

	private String orderId;
	private String name;
	private int qty;
	private double price;

	public Order() {
	}

	public Order(String orderId, String name, int qty, double price) {
		this.orderId = orderId;
		this.name = name;
		this.qty = qty;
		this.price = price;
	}

}
