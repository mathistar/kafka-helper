package com.dbs.cb.kafkahelper.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka-helper")
@Getter
@Setter
public class AppConfig {
  private String topic;
  private String group;
  private String retryTopic;
  private String retryGroup;
  private String retryCronJob;
}
