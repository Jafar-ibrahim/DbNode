package org.example.dbnode;

import lombok.extern.log4j.Log4j2;
import org.example.dbnode.Indexing.IndexingManager;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
@Log4j2
@SpringBootApplication(exclude = {SecurityAutoConfiguration.class })
public class DbNodeApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(DbNodeApplication.class, args);
    }

    @Override
    public void run(String... args){
        log.info("Initializing Indexing Manager");
        IndexingManager.getInstance().init();
    }
}
