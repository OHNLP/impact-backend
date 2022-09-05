package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.Arrays;
import java.util.Map;

public class NoneOfCriterion extends CompositeCriterion {


    public NoneOfCriterion(Criterion... subcriterion) {
        super(subcriterion);
    }

    @Override
    public boolean matches(DomainResource resource) {
        for (Criterion c : subcriterion) {
            if (c.matches(resource)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public double score(Map<String, PatientScore> scoreByCriterionUID) {
        // For NOT, simply invert the average score of the children (similar to AND scoring)
        return -1 * Arrays.stream(subcriterion).map(c -> c.score(scoreByCriterionUID)).reduce(Double::sum).orElse(0.00) / subcriterion.length;
    }
}
