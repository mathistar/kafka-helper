package com.dbs.cb.kafkahelper.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Component
@Slf4j
@RequiredArgsConstructor
public class RetryService {
  @Value("${app.retry-topic}")
  private String retryTopic;
  private final KafkaListenerEndpointRegistry endpointRegistry;
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private static final String RETRY_CONTAINER_ID = "retryContainer";
  private static final String EXECUTION_TIME = "executionTime";
  private static final String TARGET_TOPIC = "targetTopic";



  ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  @KafkaListener(id= RETRY_CONTAINER_ID, topics = {"${app.retry-topic}"}, groupId = "${app.retry-group}", autoStartup = "false")
  public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {

    log.info("Customer in retry : {} ", record.key());
    Map<String, String> headerMap = StreamSupport.stream(record.headers().spliterator(), true).collect(
      Collectors.toMap(Header::key, h -> new String(h.value()))
    );
    LocalDateTime executionTime = LocalDateTime.parse(headerMap.get(EXECUTION_TIME)).minusSeconds(1);
    if (LocalDateTime.now().isAfter(executionTime)) {
      String topic = headerMap.get(TARGET_TOPIC);
      ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, record.key(), record.value());
      kafkaTemplate.send(producerRecord).addCallback (
        (s) -> {
          log.info("success message send to original topic - {}", topic);
        },
        ex -> log.error("Error Sending the Message into the original topic  {} ", topic, ex)
      );
      acknowledgment.acknowledge();
    } else {
      endpointRegistry.getListenerContainer(RETRY_CONTAINER_ID).stop();
    }
  }

  public void processAfter(long delay, TimeUnit timeUnit, String topic, String messageKey, Object messageValue) {
    LocalDateTime dateTime = LocalDateTime.now().plus(delay, timeUnit.toChronoUnit());
    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(retryTopic, messageKey, messageValue);
    producerRecord.headers().add(new RecordHeader(EXECUTION_TIME, dateTime.toString().getBytes()));
    producerRecord.headers().add(new RecordHeader(TARGET_TOPIC, topic.getBytes()));
    kafkaTemplate.send(producerRecord).addCallback (
      (s) -> {
        scheduledExecutorService.schedule(this::startRetryListener, delay, timeUnit);
        log.info("success message send to retry - " + messageKey);
      },
      ex -> log.error("Error Sending the Message into the retry queue {} ", retryTopic, ex)
    );
  }

  @Scheduled(cron = "${app.retry-cron-job}")
  public void startRetryListener() {
    endpointRegistry.getListenerContainer(RETRY_CONTAINER_ID).start();
  }

}
