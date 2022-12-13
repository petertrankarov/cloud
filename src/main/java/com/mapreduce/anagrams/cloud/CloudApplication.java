package com.mapreduce.anagrams.cloud;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class CloudApplication {

	public static void main(String[] args) throws IOException {

		SpringApplication.run(CloudApplication.class, args);
	
	}

	@Value("${NAME:World}")
  	String name;

  	@RestController
  	class HelloworldController {
    	@GetMapping("/")
    	String hello() {
      		return "Hello " + name + "!";
    	}
  	}

}
