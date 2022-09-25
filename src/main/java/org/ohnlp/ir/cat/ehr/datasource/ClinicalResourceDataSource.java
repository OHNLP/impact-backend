package org.ohnlp.ir.cat.ehr.datasource;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.ehr.EHRResourceProvider;
import org.ohnlp.cat.common.impl.ehr.OHDSICDMResourceProvider;
import org.ohnlp.ir.cat.connections.DataConnection;
import org.ohnlp.ir.cat.connections.JDBCDataConnectionImpl;

import java.util.Map;

public class ClinicalResourceDataSource {
    private EHRResourceProvider resourceProvider;
    private DataConnection ehrDataConnection;


    public PCollection<DomainResource> getResources(Pipeline pipeline, ClinicalEntityType type) {
        SerializableFunction<Row, DomainResource> mapFunc = resourceProvider.getRowToResourceMapper(type);
        return ehrDataConnection.getForQueryAndSchema(
                pipeline,
                resourceProvider.getQuery(type),
                resourceProvider.getQuerySchema(type),
                resourceProvider.getIndexableIDColumnName(type)
        ).apply("Map " + type.name() + " to FHIR Resources", ParDo.of(new DoFn<Row, DomainResource>() {
            @ProcessElement
            public void process(ProcessContext pc) {
                pc.output(mapFunc.apply(pc.element()));
            }
        }));
    }

    public EHRResourceProvider getResourceProvider() {
        return resourceProvider;
    }

    public void setResourceProvider(EHRResourceProvider resourceProvider) {
        this.resourceProvider = resourceProvider;
    }

    public DataConnection getEhrDataConnection() {
        return ehrDataConnection;
    }

    public void setEhrDataConnection(DataConnection ehrDataConnection) {
        this.ehrDataConnection = ehrDataConnection;
    }
}
