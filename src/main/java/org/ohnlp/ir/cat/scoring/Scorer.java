package org.ohnlp.ir.cat.scoring;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.ohnlp.ir.cat.criteria.CriterionValue;
import org.ohnlp.ir.cat.ehr.datasource.EHRDataSource;
import org.ohnlp.ir.cat.structs.ClinicalDataType;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.Map;
import java.util.Set;

public abstract class Scorer {

    protected final EHRDataSource dataSource;
    protected final ClinicalDataType queryType;

    public Scorer(EHRDataSource dataSource, ClinicalDataType queryType) {
        this.dataSource = dataSource;
        this.queryType = queryType;
    }

    public abstract PCollection<KV<String, PatientScore>> score(Pipeline p, Map<String, Set<CriterionValue>> query);
}
