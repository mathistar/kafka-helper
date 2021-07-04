package com.dbs.cb.kafkahelper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaHelperApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaHelperApplication.class, args);
	}

}
