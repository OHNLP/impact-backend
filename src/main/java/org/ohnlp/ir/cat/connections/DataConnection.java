package org.ohnlp.ir.cat.connections;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface DataConnection {
    /**
     * Loads data connection settings from configuration
     * @param node The configuration node
     */
    void loadConfig(JsonNode node);

    /**
     * Returns a Row PCollection using a given query and schema
     * @param pipeline The pipeline object that this data retrieval is ran on
     * @param query The query to run in SQL syntax
     * @param schema The Schema of the result rows
     * @param idCol A numeric identifier column for returned results in the source data system, used for partitioning
     * @return A parallelized collection of {@link Row}s representing the query results
     */
    PCollection<Row> getForQueryAndSchema(Pipeline pipeline, String query, Schema schema, String idCol);
    void write(String table, PCollection<Row> data);
}
