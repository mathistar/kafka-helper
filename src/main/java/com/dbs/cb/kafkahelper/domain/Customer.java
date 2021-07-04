package com.dbs.cb.kafkahelper.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Customer {
  private Integer id;
  private String name;
  private LocalDate dateOfBirth;
}
