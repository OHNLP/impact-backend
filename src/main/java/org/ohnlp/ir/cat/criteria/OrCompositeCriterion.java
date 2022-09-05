package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.Arrays;
import java.util.Map;

public class OrCompositeCriterion extends CompositeCriterion {

    private final int minMatch;

    public OrCompositeCriterion(int minMatch, Criterion... subcriterion) {
        super(subcriterion);
        this.minMatch = minMatch;
    }

    @Override
    public boolean matches(DomainResource resource) {
        int match = 0;
        for (Criterion val : subcriterion) {
            if (val.matches(resource)) {
                match++;
                if (match == minMatch) {
                    return true;
                }
            }
        }
        return false;
    }

    // For OR, return max of subscores // TODO handle minimum number of matches not being met
    @Override
    public double score(Map<String, PatientScore> scoreByCriterionUID) {
        return Arrays.stream(subcriterion).map(c -> c.score(scoreByCriterionUID)).reduce(Double::max).orElse(0.00);
    }
}
