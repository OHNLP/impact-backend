package org.ohnlp.ir.cat.connections;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class JDBCDataConnectionImpl implements DataConnection {

    private int numPartitions;
    private JdbcIO.DataSourceConfiguration config;

    @Override
    public void loadConfig(JsonNode node) {
        this.numPartitions = node.has("partitions") ? node.get("partitions").asInt() : 1;
        String driverClass = node.get("driverClass").asText();
        String jdbcURL = node.get("url").asText();
        this.config = JdbcIO.DataSourceConfiguration.create(driverClass, jdbcURL);
        if (node.has("user")) {
            String user = node.get("user").asText();
            String pass = node.get("password").asText();
            config = config.withUsername(user).withPassword(pass);
        }
    }

    @Override
    public PCollection<Row> getForQueryAndSchema(Pipeline pipeline, String query, Schema schema, String idCol) {
        JdbcIO.ReadWithPartitions<Row, Long> partitionRead = JdbcIO.<Row>readWithPartitions()
                .withTable("(" + query + ") as subq")
                .withDataSourceConfiguration(config)
                .withRowMapper(rs -> {
                    List<Object> vals = new ArrayList<>();
                    schema.getFieldNames().forEach(f -> {
                        try {
                            Object val = null;
                            Schema.FieldType type = schema.getField(f).getType();
                            switch (type.getTypeName()) {
                                case BYTE:
                                    val = rs.getByte(f);
                                    break;
                                case INT16:
                                    val = rs.getShort(f);
                                    break;
                                case INT32:
                                    val = rs.getInt(f);
                                    break;
                                case INT64:
                                    val = rs.getLong(f);
                                    break;
                                case DECIMAL:
                                    val = rs.getFloat(f); // TODO (?)
                                    break;
                                case FLOAT:
                                    val = rs.getFloat(f);
                                    break;
                                case DOUBLE:
                                    val = rs.getDouble(f);
                                    break;
                                case STRING:
                                    val = rs.getString(f);
                                    break;
                                case DATETIME:
                                    val = DateTime.parse(rs.getString(f));
//                                    val = new DateTime(rs.getDate(f).getTime());
                                    break;
                                case BOOLEAN:
                                    val = rs.getBoolean(f);
                                    break;
                                case BYTES:
                                    val = rs.getBytes(f);
                                    break;
                                case ARRAY:
                                case ITERABLE:
                                case MAP:
                                case ROW:
                                case LOGICAL_TYPE:
                                    throw new UnsupportedOperationException("Unsupported sql query return type " + type.getTypeName());
                            }
                            vals.add(val);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    return Row.withSchema(schema).addValues(vals.toArray()).build();
                }).withCoder(RowCoder.of(schema));
        if (idCol != null) {
            partitionRead = partitionRead.withPartitionColumn(idCol).withNumPartitions(this.numPartitions);
        }
        return pipeline.apply("Extract from JDBC " + query, partitionRead).setRowSchema(schema);
    }

    @Override
    public void write(String table, PCollection<Row> data) {
        // Dynamically create insert statement
        String[] fields = data.getSchema().getFieldNames().toArray(new String[0]);
        String insertPs = "INSERT INTO " + table + "(" + String.join(",", fields) + ") VALUES ("
        + String.join(",", Arrays.stream(fields).map(s -> "?").collect(Collectors.toSet())) + ")";
        // And now dynamically write rows using setObject type inferences
        data.apply("Write to JDBC", JdbcIO.<Row>write().withDataSourceConfiguration(config).withStatement(insertPs).withPreparedStatementSetter((r, preparedStatement) -> {
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
