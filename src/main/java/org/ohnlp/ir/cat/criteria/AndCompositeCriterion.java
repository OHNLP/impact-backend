package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;

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
}
