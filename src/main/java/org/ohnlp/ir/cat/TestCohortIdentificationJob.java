package org.ohnlp.ir.cat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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
import org.ohnlp.ir.cat.connections.DataConnection;
import org.ohnlp.ir.cat.ehr.datasource.ClinicalResourceDataSource;
import org.ohnlp.ir.cat.scoring.BM25Scorer;
import org.ohnlp.ir.cat.scoring.Scorer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.StreamSupport;

import static org.ohnlp.ir.cat.CohortIdentificationJob.getLeafsByDataType;
import static org.ohnlp.ir.cat.CohortIdentificationJob.instantiateZeroArgumentConstructorClass;

/**
 * A sample cohort identification job using hardcoded/non-configurable settings
 */
public class TestCohortIdentificationJob {
    public static void main(String... args) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // TODO split components here into individual tests
        // First we make sure we can successfully load the criterion definition
        ObjectMapper om = new ObjectMapper();
        Criterion criterion = om.readValue(TestCohortIdentificationJob.class.getResourceAsStream("/sample_criterion.json"), Criterion.class);
        System.out.println("Executing pipeline with criterion:\r\n " + om.writerWithDefaultPrettyPrinter().writeValueAsString(criterion));
        System.out.println("==============\r\n");

        // Read in Pipeline Options
        PipelineOptionsFactory.register(JobConfiguration.class);
        JobConfiguration jobConfig = PipelineOptionsFactory.fromArgs(args).create().as(JobConfiguration.class);
        UUID jobUID = jobConfig.getJobid();
        /// Provision resource providers/connections and other required classes from config
        Pipeline p = Pipeline.create(jobConfig);
        Map<String, ClinicalResourceDataSource> resourceDataSources = new HashMap<>();
        JsonNode config = om.readTree(CohortIdentificationJob.class.getResourceAsStream("/config.json"));
        config.get("resourceProviders").fields().forEachRemaining(
                (e) -> {
                    String id = e.getKey();
                    JsonNode settings = e.getValue();
                    JsonNode provider = settings.get("provider");
                    JsonNode connection = settings.get("connection");
                    try {
                        ResourceProvider providerInstance = (ResourceProvider) instantiateZeroArgumentConstructorClass(provider.get("class").asText());
                        providerInstance.init(om.convertValue(provider.get("config"), new TypeReference<Map<String, Object>>() {
                        }));
                        DataConnection connectionInstance = (DataConnection) instantiateZeroArgumentConstructorClass(connection.get("class").asText());
                        connectionInstance.loadConfig(connection.get("config"));
                        resourceDataSources.put(id, new ClinicalResourceDataSource(providerInstance, connectionInstance));
                    } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException |
                             InstantiationException | IllegalAccessException ex) {
                        throw new RuntimeException(ex);
                    }
                }
        );
        JsonNode outputSettings = config.get("resultsConnection");
        DataConnection resultsConnection = (DataConnection) instantiateZeroArgumentConstructorClass(outputSettings.get("class").asText());
        resultsConnection.loadConfig(outputSettings.get("config"));

        try {
            // Now actually run the pipeline
            Scorer scorer = new BM25Scorer(); // TODO this should be configurable
            // Get leafs by data type
            Map<ClinicalEntityType, Map<String, EntityCriterion>> leafsByDataType = getLeafsByDataType(criterion);
            System.out.println("Identified Leaf Nodes:");
            System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(leafsByDataType));
            System.out.println("==============\r\n");

            // Perform base BM25 Scoring over each data type
            List<PCollection<KV<KV<String, String>, CandidateScore>>> leafScoreList = new ArrayList<>();
            for (Map.Entry<ClinicalEntityType, Map<String, EntityCriterion>> e : leafsByDataType.entrySet()) {
                ClinicalEntityType cdt = e.getKey();
                Map<String, EntityCriterion> leaves = e.getValue();
                // Score for each data source
                resourceDataSources.forEach((id, src) -> {
                    PCollection<KV<KV<String, String>, CandidateScore>> scores = scorer.score(p, leaves, cdt, src);
                    leafScoreList.add(scores);
                });
                // TODO combine with nlp results here
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
                                            .forEach(e -> leafScores.merge(UUID.fromString(e.getKey()), e.getValue(), (v1, v2) -> {
                                                CandidateScore score = new CandidateScore();
                                                score.setScore(v1.getScore() + v2.getScore());
                                                score.setPatientUID(v1.getPatientUID());
                                                score.setEvidenceIDs(new HashSet<>(v1.getEvidenceIDs()));
                                                score.getEvidenceIDs().addAll(v2.getEvidenceIDs());
                                                score.setDataSourceCount(v1.getDataSourceCount() + v2.getDataSourceCount());
                                                return score;
                                            }));
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
                    Schema.Field.of("evidence_uid", Schema.FieldType.STRING),
                    Schema.Field.of("score", Schema.FieldType.DOUBLE)
            );
            resultsConnection.write("cat.cohort", scoresByPatientUid.apply(ParDo.of(
                    new DoFn<KV<String, Double>, Row>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                            c.output(Row.withSchema(scoreSchema).addValues(jobUID.toString().toUpperCase(Locale.ROOT), c.element().getKey(), c.element().getValue()).build());
                        }
                    }
            )).setCoder(RowCoder.of(scoreSchema)).setRowSchema(scoreSchema));
            resultsConnection.write("cat.evidence", leafScores.apply(ParDo.of(
                    new DoFn<KV<KV<String, String>, CandidateScore>, Row>() {
                        @ProcessElement
                        public void process(ProcessContext c) {
                            String criterionUID = c.element().getKey().getKey().toUpperCase(Locale.ROOT);
                            String patientUID = c.element().getKey().getValue();
                            CandidateScore leafScore = c.element().getValue();
                            for (String id : leafScore.getEvidenceIDs()) {
                                c.output(
                                        Row.withSchema(evidenceSchema)
                                                .addValues(jobUID.toString().toUpperCase(Locale.ROOT), criterionUID, patientUID, id, leafScore.getScore() / leafScore.getDataSourceCount())
                                                .build()
                                );
                            }
                        }
                    }
            )).setCoder(RowCoder.of(evidenceSchema)).setRowSchema(evidenceSchema));
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
        p.run().waitUntilFinish();
    }
}
