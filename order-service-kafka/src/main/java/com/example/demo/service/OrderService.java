package com.example.demo.service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.demo.dto.OrderEvent;
import com.example.demo.dto.OrderRequest;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class OrderService {

	private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

	private final Map<String, OrderEvent> orders = new ConcurrentHashMap<>();

	public OrderEvent createOrder(OrderRequest request) {

		String orderId = UUID.randomUUID().toString();

		OrderEvent event = new OrderEvent(
				orderId, 
				request.getCustomerId(), 
				request.getAmount(),
				request.getProductName(), 
				"CREATED");

		orders.put(orderId, event);

		kafkaTemplate.send("order-events", orderId, event);

		return event;
	}

	public OrderEvent getOrder(String orderId) {
		return orders.get(orderId);
	}

	public Map<String, OrderEvent> getAllOrders() {
		return orders;
	}
}
