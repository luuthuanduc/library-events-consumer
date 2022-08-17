package com.aimatrix.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Data
@Builder
@Entity
@NoArgsConstructor
@AllArgsConstructor
public class FailureRecord {

    @Id
    @GeneratedValue
    private Integer bookId;
    private String topic;
    private Integer recordKey;
    private String errorRecord;
    private Integer partition;
    private Long offsetValue;
    private String exception;
    private String status;

}
