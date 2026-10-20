package com.example.demo;

import com.example.demo.dto.UserResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

@Service
public class UserService {

    private final RestClient restClient;

    public UserService(
            @Value("${user.service.url}") String userServiceUrl) {

        this.restClient = RestClient.builder()
                .baseUrl(userServiceUrl)
                .build();
    }

    public UserResponse getUserById(Long userId) {

        return restClient.get()
                .uri("/api/users/{id}", userId)
                .retrieve()
                .body(UserResponse.class);
    }
}