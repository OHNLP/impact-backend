package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;

import java.io.Serializable;

public abstract class Criterion implements Serializable {
    public abstract boolean matches(DomainResource resource);
}
