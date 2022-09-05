package org.ohnlp.ir.cat.criteria;

import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.Arrays;
import java.util.Map;

public abstract class CompositeCriterion extends Criterion {

    protected final Criterion[] subcriterion;

    public CompositeCriterion(Criterion... subcriterion) {
        this.subcriterion = subcriterion;
    }


}
