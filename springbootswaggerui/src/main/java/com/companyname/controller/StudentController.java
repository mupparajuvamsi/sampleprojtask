package com.companyname.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StudentController {
	@PostMapping("/registerStudent")
	String registerUser(@RequestParam String username,@RequestParam String password) {
		return username+password;
	}
}
