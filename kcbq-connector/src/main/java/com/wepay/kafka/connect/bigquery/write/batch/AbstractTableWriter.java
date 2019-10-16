package com.wepay.kafka.connect.bigquery.write.batch;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;

import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.FieldNameSanitizer;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Abstract Table Writer that defines common functionality for Table Writer.
 */
public abstract class AbstractTableWriter {

  protected RecordConverter<Map<String, Object>> recordConverter;
  protected SchemaManager schemaManager;
  protected boolean sanitizeData;

  protected void attemptSchemaUpdate(PartitionedTableId tableId, String topic,
      List<SinkRecord> sinkRecords) {
    try {
      schemaManager.updateSchema(tableId.getBaseTableId(), topic, sinkRecords);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
          "Failed to update table schema for: " + tableId.getBaseTableId(), exception);
    }
  }

  protected RowToInsert getRecordRow(SinkRecord record) {
    Map<String,Object> convertedRecord = this.recordConverter.convertRecord(record);
    if (this.sanitizeData) {
      convertedRecord = FieldNameSanitizer.replaceInvalidKeys(convertedRecord);
    }

    return RowToInsert.of(getRowId(record), convertedRecord);
  }

  protected String getRowId(SinkRecord record) {
    return String.format("%s-%d-%d",
        record.topic(),
        record.kafkaPartition(),
        record.kafkaOffset());
  }
}
