package com.aimatrix.service;

import com.aimatrix.entity.FailureRecord;
import com.aimatrix.jpa.FailureRecordRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
public class FailureRecordService {

    final FailureRecordRepository failureRecordRepository;

    public FailureRecordService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus) {
        failureRecordRepository.save(new FailureRecord(
                null, record.topic(), record.key(), record.value(), record.partition(), record.offset(),
                exception.getCause().getMessage(), recordStatus));
    }
}
