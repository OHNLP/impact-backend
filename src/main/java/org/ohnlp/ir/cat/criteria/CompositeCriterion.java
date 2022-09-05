package org.ohnlp.ir.cat.criteria;

public abstract class CompositeCriterion extends Criterion {

    protected final Criterion[] subcriterion;

    public CompositeCriterion(Criterion... subcriterion) {
        this.subcriterion = subcriterion;
    }
}
