package org.ohnlp.ir.cat.ehr.datasource;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.EntityCriterion;
import org.ohnlp.cat.api.criteria.EntityValue;

import java.util.Set;

public interface EHRDataSource {
    void loadConfig(JsonNode node);
    Set<EntityValue> convertToLocalTerminology(ClinicalEntityType type, EntityValue input);
    PCollection<Person> getPersons(Pipeline pipeline);
    PCollection<Condition> getConditions(Pipeline pipeline);
    PCollection<MedicationStatement> getMedications(Pipeline pipeline);
    PCollection<Procedure> getProcedures(Pipeline pipeline);
    PCollection<Observation> getObservations(Pipeline pipeline);
}
