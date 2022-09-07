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

    /**
     * Scores a patient collection on a given query
     * @param p The pipeline to use
     * @param query A mapping of criterion UIDs to {@link CriterionValue}. These should be leaf values/non-compositional.
     *              It is expected that base CriterionValues have already been converted to local implementation vocabulary
     *              values as appropriate using {@link EHRDataSource#convertToLocalTerminology(ClinicalDataType, CriterionValue)}
     * @param queryType The data type referenced by query objects
     * @param dataSource The Data source to use for this query
     * @return A mapping of ((criterion_uid, patient_uid), {@link PatientScore}) results
     */
    public abstract PCollection<KV<KV<String, String>, PatientScore>> score(Pipeline p, Map<String, Set<CriterionValue>> query, ClinicalDataType queryType, EHRDataSource dataSource);
}
