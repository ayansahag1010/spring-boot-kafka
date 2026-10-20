package com.example.demo;

import com.example.demo.dto.OrderEvent;
import com.example.demo.dto.UserResponse;
import lombok.RequiredArgsConstructor;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

	private final UserService userService;
	private final EmailService emailService;

	@KafkaListener(topics = "order-events")
	public void consumeOrderEvent(OrderEvent event) {

		System.out.println("Order received: " + event.getOrderId());

		UserResponse user = userService.getUserById(event.getCustomerId());

		if (user == null || user.getEmail() == null || user.getEmail().isBlank()) {
			System.out.println("User email not found!");
			return;
		}

		emailService.sendOrderConfirmation(user.getEmail(), event.getOrderId(), event.getProductName(),
				String.valueOf(event.getAmount()));

		System.out.println("Confirmation email sent to: " + user.getEmail());
	}
}