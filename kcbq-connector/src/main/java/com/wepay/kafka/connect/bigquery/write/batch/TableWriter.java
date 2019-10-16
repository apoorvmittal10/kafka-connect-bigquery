package com.wepay.kafka.connect.bigquery.write.batch;

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


import com.google.cloud.bigquery.BigQueryException;

import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.InvalidSchemaException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple Table Writer that attempts to write all the rows it is given at once.
 */
public class TableWriter extends AbstractTableWriter implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(TableWriter.class);

  private static final int BAD_REQUEST_CODE = 400;
  private static final String INVALID_REASON = "invalid";
  // The maximum number of retries we will attempt to write rows after updating a BQ table schema.
  private static final int AFTER_UPDATE_RETY_LIMIT = 5;

  private final BigQueryWriter writer;
  private final PartitionedTableId table;
  private final List<SinkRecord> rows;
  private final String topic;

  /**
   * @param writer the {@link BigQueryWriter} to use.
   * @param table the BigQuery table to write to.
   * @param rows the rows to write.
   * @param topic the kafka source topic of this data.
   * @param recordConverter the {@link RecordConverter} used to convert records to rows.
   * @param schemaManager the {@link SchemaManager} used to update BigQuery tables.
   * @param sanitizeData the boolean specifying whether to sanitize data before persisting.
   */
  public TableWriter(BigQueryWriter writer,
                     PartitionedTableId table,
                     List<SinkRecord> rows,
                     String topic,
                     RecordConverter<Map<String, Object>> recordConverter,
                     SchemaManager schemaManager,
                     boolean sanitizeData) {
    this.writer = writer;
    this.table = table;
    this.rows = rows;
    this.topic = topic;

    this.recordConverter = recordConverter;
    this.schemaManager = schemaManager;
    this.sanitizeData = sanitizeData;
  }

  @Override
  public void run() {
    int currentIndex = 0;
    int currentBatchSize = rows.size();
    int successCount = 0;
    int failureCount = 0;
    int invalidSchemaErrorCount = 0;
    boolean schemaUpdateExecuted = false;

    try {
      while (currentIndex < rows.size()) {
        List<SinkRecord> currentBatch =
            rows.subList(currentIndex, Math.min(currentIndex + currentBatchSize, rows.size()));
        try {
          writer.writeRows(table, currentBatch.stream().map(r -> getRecordRow(r)).
              collect(Collectors.toList()), topic);
          currentIndex += currentBatchSize;
          successCount++;
          // Set schema update to false as schema exception might occur for next batch.
          schemaUpdateExecuted = false;
          invalidSchemaErrorCount = 0;
        } catch (BigQueryException err) {
          if (isBatchSizeError(err)) {
            failureCount++;
            currentBatchSize = getNewBatchSize(currentBatchSize);
          }
        } catch (InvalidSchemaException err) {
          if (invalidSchemaErrorCount > AFTER_UPDATE_RETY_LIMIT) {
            throw new BigQueryConnectException(
                "Failed to write rows after BQ schema update within "
                    + AFTER_UPDATE_RETY_LIMIT + " attempts for: " + table.getBaseTableId());
          }

          invalidSchemaErrorCount++;
          // try updating new schema if not already done.
          if (!schemaUpdateExecuted) {
            attemptSchemaUpdate(this.table, this.topic, currentBatch);
            schemaUpdateExecuted = true;
          }
        }

      }
    } catch (InterruptedException err) {
      throw new ConnectException("Thread interrupted while writing to BigQuery.", err);
    }

    // Common case is 1 successful call and 0 failed calls:
    // Write to info if uncommon case,
    // Write to debug if common case
    String logMessage = "Wrote {} rows over {} successful calls and {} failed calls.";
    if (successCount + failureCount > 1) {
      logger.info(logMessage, rows.size(), successCount, failureCount);
    } else {
      logger.debug(logMessage, rows.size(), successCount, failureCount);
    }
  }

  private static int getNewBatchSize(int currentBatchSize) {
    if (currentBatchSize == 1) {
      // todo correct exception type?
      throw new ConnectException("Attempted to reduce batch size below 1.");
    }
    // round batch size up so we don't end up with a dangling 1 row at the end.
    return (int) Math.ceil(currentBatchSize / 2.0);
  }

  /**
   * @param exception the {@link BigQueryException} to check.
   * @return true if this error is an error that can be fixed by retrying with a smaller batch
   *         size, or false otherwise.
   */
  private static boolean isBatchSizeError(BigQueryException exception) {
    if (exception.getCode() == BAD_REQUEST_CODE
        && exception.getError() == null
        && exception.getReason() == null) {
      /*
       * 400 with no error or reason represents a request that is more than 10MB. This is not
       * documented but is referenced slightly under "Error codes" here:
       * https://cloud.google.com/bigquery/quota-policy
       * (by decreasing the batch size we can eventually expect to end up with a request under 10MB)
       */
      return true;
    } else if (exception.getCode() == BAD_REQUEST_CODE
               && INVALID_REASON.equals(exception.getReason())) {
      /*
       * this is the error that the documentation claims google will return if a request exceeds
       * 10MB. if this actually ever happens...
       * todo distinguish this from other invalids (like invalid table schema).
       */
      return true;
    }
    return false;
  }

  public String getTopic() {
    return topic;
  }

  public static class Builder implements TableWriterBuilder {
    private final BigQueryWriter writer;

    private final PartitionedTableId table;
    private final String topic;

    private final List<SinkRecord> rows;

    private final RecordConverter<Map<String, Object>> recordConverter;

    private final SchemaManager schemaManager;
    private final boolean sanitizeData;

    /**
     * @param writer the BigQueryWriter to use
     * @param table the BigQuery table to write to.
     * @param topic the kafka source topic associated with the given table.
     * @param recordConverter the {@link RecordConverter} used to convert records to rows.
     * @param schemaManager the {@link SchemaManager} used to update BigQuery tables.
     * @param sanitizeData the boolean specifying whether to sanitize data before persisting.
     */
    public Builder(BigQueryWriter writer, PartitionedTableId table, String topic,
                   RecordConverter<Map<String, Object>> recordConverter,
                   SchemaManager schemaManager,
                   boolean sanitizeData) {
      this.writer = writer;
      this.table = table;
      this.topic = topic;

      this.rows = new ArrayList<>();

      this.recordConverter = recordConverter;
      this.schemaManager = schemaManager;
      this.sanitizeData = sanitizeData;
    }

    @Override
    public void addRow(SinkRecord record) {
      rows.add(record);
    }

    @Override
    public TableWriter build() {
      return new TableWriter(writer, table, rows, topic, recordConverter, schemaManager,
          sanitizeData);
    }
  }
}
