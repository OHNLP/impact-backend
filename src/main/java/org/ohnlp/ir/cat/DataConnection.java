package org.ohnlp.ir.cat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface DataConnection {
    PCollection<Row> getForQueryAndSchema(Pipeline pipeline, String query, Schema schema);
}
