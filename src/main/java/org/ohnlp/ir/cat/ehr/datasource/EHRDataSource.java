package org.ohnlp.ir.cat.ehr.datasource;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.ir.cat.criteria.CriterionValue;
import org.ohnlp.ir.cat.structs.ClinicalDataType;

import java.util.Set;

public interface EHRDataSource {
    Set<CriterionValue> convertToLocalTerminology(ClinicalDataType type, CriterionValue input);
    PCollection<Condition> getConditions(Pipeline pipeline);
    PCollection<MedicationStatement> getMedications(Pipeline pipeline);
    PCollection<Procedure> getProcedures(Pipeline pipeline);
    PCollection<Observation> getObservations(Pipeline pipeline);
}
