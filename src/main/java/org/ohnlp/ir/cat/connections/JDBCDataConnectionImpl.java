package org.ohnlp.ir.cat.connections;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.ReadableDateTime;
import org.ohnlp.ir.cat.DataConnection;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JDBCDataConnectionImpl implements DataConnection {

    private int numPartitions;

    @Override
    public void loadConfig(JsonNode node) {
        this.numPartitions = 48;
        //TODO
    }

    @Override
    public PCollection<Row> getForQueryAndSchema(Pipeline pipeline, String query, Schema schema, String idCol) {
        JdbcIO.ReadWithPartitions<Row, Long> partitionRead = JdbcIO.<Row>readWithPartitions()
                .withTable(query)
                .withRowOutput();
        if (idCol != null) {
            partitionRead = partitionRead.withPartitionColumn(idCol).withNumPartitions(this.numPartitions);
        }
        return pipeline.apply("Extract from JDBC", partitionRead);
    }

    @Override
    public void write(String table, PCollection<Row> data) {
        // Dynamically create insert statement
        String[] fields = data.getSchema().getFieldNames().toArray(new String[0]);
        String insertPs = "INSERT INTO " + table + "(" + String.join(",", fields) + ") VALUES ("
        + String.join(",", Arrays.stream(fields).map(s -> "?").collect(Collectors.toSet())) + ")";
        // And now dynamically write rows using setObject type inferences
        data.apply("Write to JDBC", JdbcIO.<Row>write().withStatement(insertPs).withPreparedStatementSetter((r, preparedStatement) -> {
            for (int i = 0; i < fields.length; i++) {
                Object value = r.getValue(fields[i]);
                if (value instanceof ReadableDateTime) {
                    value = new Date(((ReadableDateTime) value).getMillis());
                }
                preparedStatement.setObject(i + 1, value); // JDBC driver should dynamically determine type at runtime to insert
            }
        }));
    }
}
