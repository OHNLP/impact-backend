package org.ohnlp.ir.cat.connections;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row ;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class FileBasedDataConnectionImpl implements DataConnection {
    private String path;

    @Override
    public void loadConfig(JsonNode node) {
        this.path = node.get("path").asText();
    }

    @Override
    public PCollection<Row> getForQueryAndSchema(Pipeline pipeline, String query, Schema schema, String idCol) {
        throw new UnsupportedOperationException("Read from File not implemented yet");
    }

    @Override
    public void write(String table, PCollection<Row> data) {
        Schema inputSchema = data.getSchema();
        data.apply("Format " + table + " to CSV", ParDo.of(new RowToCSVDelimitedStringSink(inputSchema, ",")))
                .apply("Write to " + table, TextIO.write().to(this.path + table));
    }

    public static class RowToCSVDelimitedStringSink extends DoFn<Row, String> {
        private Schema schema;
        private String delimiter;

        private transient ThreadLocal<SimpleDateFormat> sdf = null;

        public RowToCSVDelimitedStringSink(Schema schema, String delimiter) {
            this.schema = schema;
            this.delimiter = delimiter;
        }

        @ProcessElement
        public void process(ProcessContext pc) throws ParseException {
            if (sdf == null) {
                sdf = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd")); // TODO init lock
            }
            Row in = pc.element();
            List<String> out = new ArrayList<>();
            for (Schema.Field f : schema.getFields()) {
                if (f.getType().getTypeName().isDateType()) {
                    out.add(in.getString(f.getName())); // Our SQLite test implementation already in this format
                } else {
                    out.add(in.getValue(f.getName()).toString());
                }
            }
            pc.output(String.join(this.delimiter, out));
        }
    }
}
