package com.example.demo.controller;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.dto.OrderEvent;
import com.example.demo.dto.OrderRequest;
import com.example.demo.service.OrderService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("api/orders")
@RequiredArgsConstructor
public class OrderController {

	private final OrderService service;

	@PostMapping
	@ResponseStatus(HttpStatus.CREATED)
	public OrderEvent createOrder(@RequestBody OrderRequest request) {
		return service.createOrder(request);
	}

	@GetMapping("/{orderId}")
	public OrderEvent getOrderById(@PathVariable String orderId) {
		return service.getOrder(orderId);
	}

	@GetMapping
	public Map<String, OrderEvent> getAllorders() {
		return service.getAllOrders();
	}

}
