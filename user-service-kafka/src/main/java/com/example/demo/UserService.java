package com.example.demo;

import java.util.Optional;

import org.springframework.stereotype.Service;

import com.example.demo.repository.UserRepository;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class UserService {

	private final UserRepository repository;

	public Optional<User> getUserById(Long id) {
		return repository.findById(id);
	}

	public User saveUser(User user) {
		return repository.save(user);
	}

}
