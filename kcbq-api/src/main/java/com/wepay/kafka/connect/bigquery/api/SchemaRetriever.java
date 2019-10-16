package com.wepay.kafka.connect.bigquery.api;

import com.google.cloud.bigquery.TableId;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;

/**
 * Interface for retrieving the most up-to-date schemas for a given BigQuery table. Used in
 * automatic table creation and schema updates.
 */
public interface SchemaRetriever {

  /**
   * Params for fetching schema for topic/record.
   */
  class Params {

    private final TableId tableId;
    private final String topic;
    private final List<? extends ConnectRecord> connectRecords;

    public Params(TableId tableId, String topic) {
      this(tableId, topic, (List<? extends ConnectRecord>) null);
    }

    public Params(TableId tableId, String topic, ConnectRecord connectRecord) {
      this(tableId, topic, Collections.singletonList(connectRecord));
    }

    public Params(TableId tableId, String topic, List<? extends ConnectRecord> connectRecords) {
      this.topic = topic;
      this.tableId = tableId;
      this.connectRecords = connectRecords;
    }

    public String topic() {
      return topic;
    }

    public TableId tableId() {
      return tableId;
    }

    public List<? extends ConnectRecord> connectRecords() {
      return connectRecords;
    }
  }

  /**
   * Called with all of the configuration settings passed to the connector via its
   * {@link org.apache.kafka.connect.sink.SinkConnector#start(Map)} method.
   * @param properties The configuration settings of the connector.
   */
  void configure(Map<String, String> properties);

  /**
   * Retrieve connect schema from connect record.
   * @param params The schema retrieve params that contains details to fetch schema.
   * @return The Schema for the given table.
   */
  Schema retrieveSchema(Params params);

  /**
   * Set the last seen schema for a given topic
   * @param table The table that will be created.
   * @param topic The topic to retrieve a schema for.
   * @param schema The last seen Kafka Connect Schema
   */
  void setLastSeenSchema(TableId table, String topic, Schema schema);
}
