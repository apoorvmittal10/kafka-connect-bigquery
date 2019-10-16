package com.wepay.kafka.connect.bigquery.exception;

/*
 * Copyright 2016 WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.cloud.bigquery.BigQueryError;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.Map;

/**
 * Class for schema exceptions that occur while interacting with BigQuery.
 */
public class InvalidSchemaException extends ConnectException {

  public InvalidSchemaException(String msg) {
    super(msg);
  }

  public InvalidSchemaException(String msg, Throwable thr) {
    super(msg, thr);
  }

  public InvalidSchemaException(Throwable thr) {
    super(thr);
  }

  public InvalidSchemaException(Map<Long, List<BigQueryError>> errors) {
    super(formatInsertAllErrors(errors));
  }

  private static String formatInsertAllErrors(Map<Long, List<BigQueryError>> errorsMap) {
    StringBuilder messageBuilder = new StringBuilder();
    messageBuilder.append("table insertion failed for the following rows:");
    for (Map.Entry<Long, List<BigQueryError>> errorsEntry : errorsMap.entrySet()) {
      for (BigQueryError error : errorsEntry.getValue()) {
        messageBuilder.append(String.format(
            "%n\t[row index %d]: %s: %s",
            errorsEntry.getKey(),
            error.getReason(),
            error.getMessage()
        ));
      }
    }
    return messageBuilder.toString();
  }
}
