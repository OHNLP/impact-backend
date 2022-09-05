package org.ohnlp.ir.cat.connections;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.ir.cat.DataConnection;

public class BigQueryDataConnectionImpl implements DataConnection {
    @Override
    public PCollection<Row> getForQueryAndSchema(Pipeline pipeline, String query, Schema schema) {
        return pipeline.apply("BigQuery: " + query,
                BigQueryIO.readTableRowsWithSchema()
                        .fromQuery(query)
        ).apply("BigQuery to Beam Row Conversion", ParDo.of(new DoFn<TableRow, Row>() {
            @ProcessElement
            public void processElement(@Element TableRow row, OutputReceiver<Row> out) {
                out.output(BigQueryUtils.toBeamRow(schema, row));
            }
        }));
    }

    @Override
    public void write(String table, PCollection<Row> data) {
        throw new UnsupportedOperationException("Not implemented yet"); // TODO
    }

}
