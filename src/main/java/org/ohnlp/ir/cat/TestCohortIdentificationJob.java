package org.ohnlp.ir.cat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.cat.api.cohorts.CandidateScore;
import org.ohnlp.cat.api.criteria.ClinicalEntityType;
import org.ohnlp.cat.api.criteria.Criterion;
import org.ohnlp.cat.api.criteria.EntityCriterion;
import org.ohnlp.cat.api.ehr.ResourceProvider;
import org.ohnlp.cat.common.impl.ehr.OHDSICDMNLPResourceProvider;
import org.ohnlp.cat.common.impl.ehr.OHDSICDMResourceProvider;
import org.ohnlp.ir.cat.connections.DataConnection;
import org.ohnlp.ir.cat.connections.JDBCDataConnectionImpl;
import org.ohnlp.ir.cat.ehr.datasource.ClinicalResourceDataSource;
import org.ohnlp.ir.cat.scoring.BM25Scorer;
import org.ohnlp.ir.cat.scoring.Scorer;

import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;

/**
 * A sample cohort identification job using hardcoded/non-configurable settings
 */
public class TestCohortIdentificationJob {
    public static void main(String... args) throws IOException {
        // TODO split components here into individual tests
        UUID jobUID = UUID.randomUUID();
        System.out.println("Executing sample cohort identification job with job UID " + jobUID);
        // First we make sure we can successfully load the criterion definition
        ObjectMapper om = new ObjectMapper();
        Criterion criterion = om.readValue(TestCohortIdentificationJob.class.getResourceAsStream("/sample_criterion.json"), Criterion.class);
        System.out.println("Executing pipeline with criterion:\r\n " + om.writerWithDefaultPrettyPrinter().writeValueAsString(criterion));
        System.out.println("==============\r\n");

        // Provision resource providers/connections and other required classes
        Pipeline p = Pipeline.create();
        ResourceProvider ehrResourceProvider = new OHDSICDMResourceProvider();
        ResourceProvider nlpResourceProvider = new OHDSICDMNLPResourceProvider();
        DataConnection ohdsiCDMConnection = new JDBCDataConnectionImpl();
        DataConnection resultsConnection = new JDBCDataConnectionImpl(); // TODO connection info

        // Construct relevant data sources
        ClinicalResourceDataSource ehrDataSource = new ClinicalResourceDataSource(ehrResourceProvider, ohdsiCDMConnection);
        ClinicalResourceDataSource nlpDataSource = new ClinicalResourceDataSource(nlpResourceProvider, ohdsiCDMConnection);

        // Get leafs by data type
        Map<ClinicalEntityType, Map<String, EntityCriterion>> leafsByDataType = CohortIdentificationJob.getLeafsByDataType(criterion);
        System.out.println("Identified Leaf Nodes:");
        System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(leafsByDataType));
        System.out.println("==============\r\n");

        // Perform base BM25 Scoring over each data type
        Scorer scorer = new BM25Scorer();
        List<PCollection<KV<KV<String, String>, CandidateScore>>> leafScoreList = new ArrayList<>();
        for (Map.Entry<ClinicalEntityType, Map<String, EntityCriterion>> e : leafsByDataType.entrySet()) {
            ClinicalEntityType cdt = e.getKey();
            Map<String, EntityCriterion> leaves = e.getValue();
            // Score based on EHR leaf nodes
            PCollection<KV<KV<String, String>, CandidateScore>> ehrScores = scorer.score(p, leaves, cdt, ehrDataSource);
            // TODO combine with nlp results here
            leafScoreList.add(ehrScores);
        }
        // Union all the leaf scores across the disparate clinical data types
        PCollection<KV<KV<String, String>, CandidateScore>> leafScores = PCollectionList.of(leafScoreList).apply(Flatten.pCollections());
        // Now combine scores using the base criterion
        PCollection<KV<String, Double>> scoresByPatientUid = leafScores.apply("Score Aggregation: Remap to (patient_uid, (criterion_uid, scoreWithEvidence))",
                ParDo.of(new DoFn<KV<KV<String, String>, CandidateScore>, KV<String, KV<String, CandidateScore>>>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        c.output(
                                KV.of(
                                        c.element().getKey().getValue(),
                                        KV.of(c.element().getKey().getKey(), c.element().getValue())
                                )
                        );
                    }
                })
        ).apply(
                "Score Aggregation: Group leaf scores by patient",
                GroupByKey.create()
        ).apply(
                "Score Aggregation: Apply logical/non-leaf criterion scores",
                ParDo.of(
                        new DoFn<KV<String, Iterable<KV<String, CandidateScore>>>, KV<String, Double>>() {
                            @ProcessElement
                            public void process(ProcessContext c) {
                                HashMap<UUID, CandidateScore> leafScores = new HashMap<>();
                                StreamSupport.stream(c.element().getValue().spliterator(), false)
                                        .forEach(e -> leafScores.put(UUID.fromString(e.getKey()), e.getValue()));
                                c.output(
                                        KV.of(
                                                c.element().getKey(), // patient_uid
                                                criterion.score(leafScores)
                                        )
                                );
                            }
                        }
                )
        );
        // Write both evidence and scores to DB
        Schema scoreSchema = Schema.of(
                Schema.Field.of("job_uid", Schema.FieldType.STRING),
                Schema.Field.of("person_uid", Schema.FieldType.STRING),
                Schema.Field.of("score", Schema.FieldType.DOUBLE)
        );
        Schema evidenceSchema = Schema.of(
                Schema.Field.of("job_uid", Schema.FieldType.STRING),
                Schema.Field.of("node_uid", Schema.FieldType.STRING),
                Schema.Field.of("person_uid", Schema.FieldType.STRING),
//                Schema.Field.of("criterion_score", Schema.FieldType.DOUBLE),
                Schema.Field.of("evidence_uid", Schema.FieldType.STRING)
        );
        resultsConnection.write("cohort", scoresByPatientUid.apply(ParDo.of(
                new DoFn<KV<String, Double>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        c.output(Row.withSchema(scoreSchema).addValues(jobUID, c.element().getKey(), c.element().getValue()).build());
                    }
                }
        )));
        resultsConnection.write("evidence", leafScores.apply(ParDo.of(
                new DoFn<KV<KV<String, String>, CandidateScore>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        String criterionUID = c.element().getKey().getKey();
                        String patientUID = c.element().getKey().getValue();
                        CandidateScore leafScore = c.element().getValue();
                        for (String id : leafScore.getEvidenceIDs()) {
                            c.output(
                                    Row.withSchema(evidenceSchema)
//                                            .addValues(jobUID, criterionUID, patientUID, leafScore.getScore(), id)
                                            .addValues(jobUID, criterionUID, patientUID, id)
                                            .build()
                            );
                        }
                    }
                }
        )));
        p.run().waitUntilFinish();
    }
}
