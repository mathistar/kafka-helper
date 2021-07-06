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
import org.springframework.kafka.listener.MessageListenerContainer;
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

  private final KafkaListenerEndpointRegistry endpointRegistry;
  private final KafkaTemplate<String, Object> kafkaTemplate;
  private static final String RETRY_CONTAINER_ID = "retryContainer";
  private static final String EXECUTION_TIME = "executionTime";
  private static final String TARGET_TOPIC = "targetTopic";

  @Value("${kafka-helper.retry-topic}")
  private String retryTopic;

  // Scheduler to restart the kafka listener
  ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

  /**
   * Kafka retry topic listener to consume message and send the same message to orginal queue
   *
   * @param record         Message object
   * @param acknowledgment to commit the offset
   */
  @KafkaListener(id = RETRY_CONTAINER_ID, topics = {"${kafka-helper.retry-topic}"}, groupId = "${kafka-helper.retry-group}")
  public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
    Map<String, String> headerMap = StreamSupport.stream(record.headers().spliterator(), true).collect(
      Collectors.toMap(Header::key, h -> new String(h.value()))
    );
    LocalDateTime executionTime = LocalDateTime.parse(headerMap.get(EXECUTION_TIME)).minusSeconds(1);
    MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(RETRY_CONTAINER_ID);
    LocalDateTime now = LocalDateTime.now();
    log.info("Message in retry : {}, now: {}, execution: {}", record.key(), now, executionTime);
    if (listenerContainer != null && now.isAfter(executionTime)) {
      log.info("sending to original topic : {} ", record.key());
      String topic = headerMap.get(TARGET_TOPIC);
      ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, record.key(), record.value());
      kafkaTemplate.send(producerRecord).addCallback(
        (s) -> log.info("successfully message send to original topic - {}", record.key()),
        ex -> log.error("Error Sending the Message into the original topic  {} ", topic, ex)
      );
      acknowledgment.acknowledge();
    }
  }

  /**
   * Method to process the Message again after the given delay in minutes
   *
   * @param delayInMinutes to delay
   * @param topic          to resend
   * @param messageKey     value of key
   * @param messageValue   value of object
   */
  public void processAfter(long delayInMinutes, String topic, String messageKey, Object messageValue) {
    LocalDateTime dateTime = LocalDateTime.now().plus(delayInMinutes, TimeUnit.MINUTES.toChronoUnit());
    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(retryTopic, messageKey, messageValue);
    producerRecord.headers().add(new RecordHeader(EXECUTION_TIME, dateTime.toString().getBytes()));
    producerRecord.headers().add(new RecordHeader(TARGET_TOPIC, topic.getBytes()));
    kafkaTemplate.send(producerRecord).addCallback(
      (s) -> scheduledExecutorService.schedule(this::startRetryListener, delayInMinutes, TimeUnit.MINUTES),
      ex -> log.error("Error Sending the Message into the retry queue {} ", retryTopic, ex)
    );
  }

  /**
   * scheduler to restart the Retry listener
   */
  @Scheduled(cron = "${kafka-helper.retry-cron-job}")
  public void startRetryListener() {
    MessageListenerContainer listenerContainer = endpointRegistry.getListenerContainer(RETRY_CONTAINER_ID);
    if (listenerContainer != null && listenerContainer.isRunning()) {
      log.info("Retry Listener will be restarted - {}", LocalDateTime.now());
      listenerContainer.stop(listenerContainer::start);
    }
  }

}
