package com.coresys.state;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableJpaRepositories(basePackages = "com.coresys.state")
public class StateApplication {
    public static void main(String[] args) {
        SpringApplication.run(StateApplication.class, args);
    }
}