package org.ohnlp.ir.cat.scoring;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.ohnlp.cat.api.cohorts.CandidateScore;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.EntityCriterion;
import org.ohnlp.cat.api.criteria.EntityValue;
import org.ohnlp.ir.cat.ehr.datasource.ClinicalResourceDataSource;

import java.io.Serializable;
import java.util.Map;

public abstract class Scorer implements Serializable {

    /**
     * Scores a patient collection on a given query
     * @param p The pipeline to use
     * @param query A mapping of criterion UIDs to {@link CandidateScore}. These should be leaf values/non-compositional.
     *              It is expected that base CriterionValues have already been converted to local implementation vocabulary
     *              and that corresponding {@link EntityValue#getExpandedCodes()} is populated
     * @param queryType The data type referenced by query objects
     * @param dataSource The Data source to use for this query
     * @return A mapping of ((criterion_uid, patient_uid), {@link CandidateScore}) results
     */
    public abstract PCollection<KV<KV<String, String>, CandidateScore>> score(Pipeline p, Map<String, EntityCriterion> query, ClinicalEntityType queryType, ClinicalResourceDataSource dataSource);
}
