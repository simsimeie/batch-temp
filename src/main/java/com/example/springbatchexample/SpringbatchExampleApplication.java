package com.example.springbatchexample;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SpringbatchExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbatchExampleApplication.class, args);
    }

}
