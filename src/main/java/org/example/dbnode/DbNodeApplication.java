package org.example.dbnode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;

@SpringBootApplication(exclude = {SecurityAutoConfiguration.class })
public class DbNodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbNodeApplication.class, args);
    }

}
