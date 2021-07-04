package com.dbs.cb.kafkahelper.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class TopicConfig {
  public static final String CUSTOMER_TOPIC = "customer-topic";
  public static final String CUSTOMER_RETRY_TOPIC = "customer-retry-topic";

  @Bean
  public NewTopic customerRetryTopics(){
    return TopicBuilder.name(CUSTOMER_RETRY_TOPIC)
      .partitions(1)
      .replicas(1)
      .build();
  }

  @Bean
  public NewTopic customerTopics(){
    return TopicBuilder.name(CUSTOMER_TOPIC)
      .partitions(3)
      .replicas(1)
      .build();
  }

}
