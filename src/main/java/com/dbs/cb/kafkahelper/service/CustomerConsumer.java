package com.dbs.cb.kafkahelper.service;

import com.dbs.cb.kafkahelper.config.TopicConfig;
import com.dbs.cb.kafkahelper.domain.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class CustomerConsumer {
  private final RetryService retryService;
  private final List<Integer> customerIds = new ArrayList<>();

  @Value("${app.topic}")
  private String topic;

  @KafkaListener(topics = {"${app.topic}"}, groupId = "${app.group}")
  public void onMessage(ConsumerRecord<String, Customer> consumerRecord, Acknowledgment acknowledgment) {
    Customer customer = consumerRecord.value();
    log.info("Customer in main  : {} ", consumerRecord.key());
    int key = Integer.parseInt(consumerRecord.key());
    if ( key % 2 == 1 && !customerIds.contains(key)) {
      customerIds.add(key);
      retryService.processAfter(60, TimeUnit.SECONDS, topic, consumerRecord.key(), customer);
    }
    acknowledgment.acknowledge();
  }
}


