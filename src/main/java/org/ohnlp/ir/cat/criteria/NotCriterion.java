package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;

public class NotCriterion extends Criterion {

    private final Criterion child;

    public NotCriterion(Criterion c) {
        this.child = c;
    }

    @Override
    public boolean matches(DomainResource resource) {
        return !child.matches(resource);
    }
}
