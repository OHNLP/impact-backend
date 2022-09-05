package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.Arrays;
import java.util.Map;

public class AndCompositeCriterion extends CompositeCriterion {

    public AndCompositeCriterion(Criterion... subcriterion) {
        super(subcriterion);
    }

    @Override
    public boolean matches(DomainResource resource) {
        for (Criterion c : subcriterion) {
            if (!c.matches(resource)) {
                return false;
            }
        }
        return true;
    }

    // For AND, simply return average of all subscores
    @Override
    public double score(Map<String, PatientScore> scoreByCriterionUID) {
        return Arrays.stream(subcriterion).map(c -> c.score(scoreByCriterionUID)).reduce(Double::sum).orElse(0.00) / subcriterion.length;
    }
}
