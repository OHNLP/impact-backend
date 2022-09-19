package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

public abstract class Criterion implements Serializable {

    protected UUID criterionUID;

    public abstract boolean matches(DomainResource resource);
    public abstract double score(Map<String, PatientScore> scoreByCriterionUID);

    public UUID getCriterionUID() {
        return criterionUID;
    }
}
