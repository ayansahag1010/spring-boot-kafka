
package com.example.demo;

import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class EmailService {

    private final JavaMailSender mailSender;

    public void sendOrderConfirmation(
            String recipientEmail,
            String orderId,
            String productName,
            String amount) {

        // Create a short, user-friendly order ID
        String displayOrderId = "ORD-" +
                orderId.substring(0, 8).toUpperCase();

        // Create the email
        SimpleMailMessage message = new SimpleMailMessage();

        message.setTo(recipientEmail);

        message.setSubject(
                "Order Confirmation - " + displayOrderId
        );

        message.setText(
                "Hello,\n\n"
                + "Your order has been created successfully.\n\n"
                + "Order ID: " + displayOrderId + "\n"
                + "Product: " + productName + "\n"
                + "Amount: " + amount + "\n\n"
                + "Thank you for your order!"
        );

        // Send the email
        mailSender.send(message);
    }
}
