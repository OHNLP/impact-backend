package org.ohnlp.ir.cat.ehr.datasource;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.ehr.ResourceProvider;
import org.ohnlp.ir.cat.connections.DataConnection;

public class ClinicalResourceDataSource {
    private ResourceProvider resourceProvider;
    private DataConnection dataConnection;


    public PCollection<DomainResource> getResources(Pipeline pipeline, ClinicalEntityType type) {
        SerializableFunction<Row, DomainResource> mapFunc = resourceProvider.getRowToResourceMapper(type);
        return dataConnection.getForQueryAndSchema(
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

    public ResourceProvider getResourceProvider() {
        return resourceProvider;
    }

    public void setResourceProvider(ResourceProvider resourceProvider) {
        this.resourceProvider = resourceProvider;
    }

    public DataConnection getDataConnection() {
        return dataConnection;
    }

    public void setDataConnection(DataConnection dataConnection) {
        this.dataConnection = dataConnection;
    }
}
