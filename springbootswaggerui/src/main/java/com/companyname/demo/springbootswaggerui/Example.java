package com.companyname.demo.springbootswaggerui;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@EnableAutoConfiguration
@ComponentScan(basePackages= {"com.companyname.demo.springbootswaggerui","com.companyname.controller"})
public class Example {

	@RequestMapping("/")
	String home() {
		return "Hello World!";
	}
	/*//http://localhost:8080/registerUser?password=krishna&username=vamsi
	@PostMapping("/registerUser")
	String registerUser(@RequestParam String username,@RequestParam String password) {
		return username+password;
	}*/

	public static void main(String[] args) {
		SpringApplication.run(Example.class, args);
	}

}