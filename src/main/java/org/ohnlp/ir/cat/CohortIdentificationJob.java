package org.ohnlp.ir.cat;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.ohnlp.ir.cat.connections.BigQueryDataConnectionImpl;
import org.ohnlp.ir.cat.criteria.Criterion;
import org.ohnlp.ir.cat.criteria.CriterionValue;
import org.ohnlp.ir.cat.ehr.datasource.EHRDataSource;
import org.ohnlp.ir.cat.ehr.datasource.OHDSICDMDataSource;
import org.ohnlp.ir.cat.scoring.BM25Scorer;
import org.ohnlp.ir.cat.scoring.Scorer;
import org.ohnlp.ir.cat.structs.ClinicalDataType;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

public class CohortIdentificationJob {
    public static void main(String... args) {
        String jobUID = ""; // TODO
        DataConnection out = new BigQueryDataConnectionImpl(); // TODO config
        EHRDataSource dataSource = new OHDSICDMDataSource();
        // TODO render init configurable/parseable
        ClinicalDataType cdt = ClinicalDataType.CONDITION; // TODO
        Criterion baseCriterion = new CriterionValue();
        Map<String, Set<CriterionValue>> queryDefinitionForCDT = new HashMap<>(); // TODO load from criterion tree and split into different data types/synonyms
        Pipeline p = Pipeline.create();
        Scorer scorer = new BM25Scorer();
        // Score based on leaf nodes
        PCollection<KV<KV<String, String>, PatientScore>> leafScores = scorer.score(p, queryDefinitionForCDT, cdt, dataSource);
        // Now combine scores using the base criterion
        PCollection<KV<String, Double>> scoresByPatientUid = leafScores.apply("Score Aggregation: Remap to (patient_uid, (criterion_uid, scoreWithEvidence))",
                ParDo.of(new DoFn<KV<KV<String, String>, PatientScore>, KV<String, KV<String, PatientScore>>>() {
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
                        new DoFn<KV<String, Iterable<KV<String, PatientScore>>>, KV<String, Double>>() {
                            @ProcessElement
                            public void process(ProcessContext c) {
                                HashMap<String, PatientScore> leafScores = new HashMap<>();
                                StreamSupport.stream(c.element().getValue().spliterator(), false)
                                        .forEach(e -> leafScores.put(e.getKey(), e.getValue()));
                                c.output(
                                        KV.of(
                                                c.element().getKey(), // patient_uid
                                                baseCriterion.score(leafScores)
                                        )
                                );
                            }
                        }
                )
        );
        // Write both evidence and scores to DB
        Schema scoreSchema = Schema.of(
                Schema.Field.of("job_uid", Schema.FieldType.STRING),
                Schema.Field.of("patient_uid", Schema.FieldType.STRING),
                Schema.Field.of("score", Schema.FieldType.DOUBLE)
        );
        Schema evidenceSchema = Schema.of(
                Schema.Field.of("job_uid", Schema.FieldType.STRING),
                Schema.Field.of("patient_uid", Schema.FieldType.STRING),
                Schema.Field.of("criterion_uid", Schema.FieldType.STRING),
                Schema.Field.of("criterion_score", Schema.FieldType.DOUBLE),
                Schema.Field.of("evidence_id", Schema.FieldType.STRING)
        );
        out.write("scores", scoresByPatientUid.apply(ParDo.of(
                new DoFn<KV<String, Double>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        c.output(Row.withSchema(scoreSchema).addValues(jobUID, c.element().getKey(), c.element().getValue()).build());
                    }
                }
        )));
        out.write("evidence", leafScores.apply(ParDo.of(
                new DoFn<KV<KV<String, String>, PatientScore>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        String criterionUID = c.element().getKey().getKey();
                        String patientUID = c.element().getKey().getValue();
                        PatientScore leafScore = c.element().getValue();
                        for (String id : leafScore.getEvidenceIDs()) {
                            c.output(
                                    Row.withSchema(evidenceSchema)
                                    .addValues(jobUID, patientUID, criterionUID, leafScore.getScore(), id)
                                    .build()
                            );
                        }
                    }
                }
        )));
        p.run().waitUntilFinish();
    }
}
