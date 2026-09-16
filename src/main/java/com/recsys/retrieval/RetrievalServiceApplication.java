package com.demo.retrieval;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class RetrievalServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(RetrievalServiceApplication.class, args);
    }
}
