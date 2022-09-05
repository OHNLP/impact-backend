package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;

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
}
