package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses the Connect Record Value Schema to fetch the schema for given record.
 */
public class ConnectRecordValueSchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(
      ConnectRecordValueSchemaRetriever.class);

  @Override
  public void configure(Map<String, String> properties) {
  }

  @Override
  public Schema retrieveSchema(Params params) {
    if (params == null || params.connectRecords() == null || params.connectRecords().isEmpty()) {
      throw new ConnectException("Fail to retrieve connect schema from record.");
    }

    // TODO (Apoorv) :  Check if value schema exists in all records.

    // Its safe to fetch first record and topic as topic shall be same for connect records being
    // inserted in same batch.
    logger.debug("Retrieving schema information for topic {}",
        params.connectRecords().get(0).topic());

    // TODO (Apoorv) :  Return merged schema from all connect records.
    return null;
  }

  @Override
  public void setLastSeenSchema(TableId table, String topic, Schema schema) {
    /* TODO (Apoorv) : Need to keep last seen copy and need to merge current and last copy in case
        of failure. As value schema is being generated respect to current value record but big
        query needs merged schema with all known fields hence merge existing and new schema.
    */
  }
}
