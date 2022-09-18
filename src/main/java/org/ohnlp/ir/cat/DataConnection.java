package org.ohnlp.ir.cat;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface DataConnection {
    void loadConfig(JsonNode node);
    PCollection<Row> getForQueryAndSchema(Pipeline pipeline, String query, Schema schema, String idCol);
    void write(String table, PCollection<Row> data);
}
