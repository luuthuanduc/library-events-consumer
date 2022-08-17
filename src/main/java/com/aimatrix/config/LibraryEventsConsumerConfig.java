package com.aimatrix.config;

import com.aimatrix.service.FailureRecordService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    public static final String RETRY = "RETRY";

    @Value("${spring.kafka.template.default-topic}")
    String topic;

    final KafkaProperties kafkaProperties;
    final FailureRecordService failureRecordService;
    final KafkaTemplate<Integer, String> kafkaTemplate;

    public LibraryEventsConsumerConfig(KafkaProperties kafkaProperties, FailureRecordService failureRecordService, KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaProperties = kafkaProperties;
        this.failureRecordService = failureRecordService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(defaultErrorHandler());
        return factory;
    }

    private DefaultErrorHandler defaultErrorHandler() {
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler(defaultDeadLetterPublishingRecover(), defaultExponentialBackOffWithMaxRetries());
        defaultErrorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        defaultErrorHandler.setRetryListeners(
                (consumerRecord, exception, deliveryAttempt) -> log.info("Failed record in retry listener exception: {} , deliveryAttempt: {}", exception.getMessage(), deliveryAttempt));
        return defaultErrorHandler;
    }

    public DeadLetterPublishingRecoverer defaultDeadLetterPublishingRecover() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (consumerRecord, exception) -> {
            log.error("Exception is: {} with failed record: {}", exception, consumerRecord);
            if (exception.getCause() instanceof RecoverableDataAccessException) {
                return new TopicPartition(topic + ".RETRY", consumerRecord.partition());
            } else {
                return new TopicPartition(topic + ".DLT", consumerRecord.partition());
            }
        });
    }

    private ConsumerRecordRecoverer defaultConsumerRecordRecover() {
        return (consumerRecord, exception) -> {
            log.error("Exception is: {} with failed record: {}", exception, consumerRecord);
            if (exception.getCause() instanceof RecoverableDataAccessException) {
                log.info("Inside the recoverable logic");
                failureRecordService.saveFailedRecord((ConsumerRecord<Integer, String>) consumerRecord, exception, RETRY);
            } else {
                log.info("Inside the non recoverable logic and skipping the record: {}", consumerRecord);
            }
        };
    }

    private ExponentialBackOffWithMaxRetries defaultExponentialBackOffWithMaxRetries() {
        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2000L);
        return expBackOff;
    }

    private FixedBackOff defaultFixedBackOff() {
        return new FixedBackOff(1000L, 2L);
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties());
    }

}
