package org.ohnlp.ir.cat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.Pipeline;
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
import org.ohnlp.cat.api.criteria.*;
import org.ohnlp.ir.cat.connections.DataConnection;
import org.ohnlp.ir.cat.criterion.SynonymExpandedEntityValues;
import org.ohnlp.ir.cat.ehr.datasource.EHRDataSource;
import org.ohnlp.ir.cat.scoring.BM25Scorer;
import org.ohnlp.ir.cat.scoring.Scorer;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;

import javax.swing.text.html.parser.Entity;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.StreamSupport;

public class CohortIdentificationJob {
    public static void main(String... args) throws IOException, ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        // Read in Pipeline Options
        PipelineOptionsFactory.register(JobConfiguration.class);
        JobConfiguration jobConfig = PipelineOptionsFactory.fromArgs(args).create().as(JobConfiguration.class);
        // Set up middleware connection
        RestTemplate middleware = new RestTemplate();
        middleware.setUriTemplateHandler(new DefaultUriBuilderFactory(jobConfig.getCallback()));
        UUID jobUID = jobConfig.getJobid();
        // Load data connections from bundled config
        JsonNode backendConfig = new ObjectMapper().readTree(CohortIdentificationJob.class.getResourceAsStream("/org/ohnlp/ir/cat/backend/config.json"));
        JsonNode ehrConfig = backendConfig.get("data").get("ehr");
        String ehrDataConnectionClazz = ehrConfig.get("connection").get("class").asText();
        DataConnection ehr = (DataConnection) instantiateZeroArgumentConstructorClass(ehrDataConnectionClazz);
        ehr.loadConfig(ehrConfig.get("connection").get("config"));
        String ehrDataSourceClazz = ehrConfig.get("dataSource").get("class").asText();
        EHRDataSource ehrDataSource = (EHRDataSource) instantiateZeroArgumentConstructorClass(ehrDataSourceClazz);;
        ehrDataSource.loadConfig(ehrConfig.get("dataSource").get("config"));
        String outputDataConnectionClazz = backendConfig.get("data").get("output").get("connection").get("class").asText();
        DataConnection out = (DataConnection) instantiateZeroArgumentConstructorClass(outputDataConnectionClazz);
        out.loadConfig(backendConfig.get("data").get("output").get("connection").get("config"));
        // Retrieve Criterion from middleware
        //TODO this should be using job_uid, not project_uid, but this functionality is not yet implemented in middleware
        Criterion criterion = middleware.getForObject(
                "/_projects/criterion?project_uid={project}",
                Criterion.class,
                Map.of("project_uid", jobUID.toString().toUpperCase(Locale.ROOT)));
        Map<ClinicalEntityType, Map<String, EntityCriterion>> leafsByDataType = getLeafsByDataType(criterion);
        // Now actually run the pipeline
        Pipeline p = Pipeline.create();
        Scorer scorer = new BM25Scorer(); // TODO this should be configurable
        // Iterate through all data types, generating scores for all leaf nodes
        List<PCollection<KV<KV<String, String>, CandidateScore>>> leafScoreList = new ArrayList<>();
        for (Map.Entry<ClinicalEntityType, Map<String, EntityCriterion>> e : leafsByDataType.entrySet()) {
            ClinicalEntityType cdt = e.getKey();
            Map<String, EntityCriterion> leaves = e.getValue();
            expandLeafNodes(cdt, leaves, ehrDataSource);
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
                new DoFn<KV<KV<String, String>, CandidateScore>, Row>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        String criterionUID = c.element().getKey().getKey();
                        String patientUID = c.element().getKey().getValue();
                        CandidateScore leafScore = c.element().getValue();
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


    private static Object instantiateZeroArgumentConstructorClass(String clazz)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        return Class.forName(clazz).getDeclaredConstructor().newInstance();
    }

    private static Map<ClinicalEntityType, Map<String, EntityCriterion>> getLeafsByDataType(Criterion criterion) {
        Map<ClinicalEntityType, Map<String, EntityCriterion>> ret = new HashMap<>();
        getLeafsByDataTypeRecurs(criterion, ret);
        return ret;
    }

    private static void getLeafsByDataTypeRecurs(Criterion criterion, Map<ClinicalEntityType, Map<String, EntityCriterion>> leafsByCDT) {
        if (criterion instanceof EntityCriterion) { // Leaf Node
            ClinicalEntityType type = ((EntityCriterion)criterion).getType();
            leafsByCDT.computeIfAbsent(type, k -> new HashMap<>())
                    .put(criterion.getNodeUID().toString().toUpperCase(Locale.ROOT), (EntityCriterion) criterion);
        } else if (criterion instanceof LogicalCriterion) {
            for (Criterion child : ((LogicalCriterion)criterion).getChildren()) {
                getLeafsByDataTypeRecurs(child, leafsByCDT);
            }
        } else {
            throw new UnsupportedOperationException("Unknown criterion type that is not a leaf or a composite");
        }
    }

    private static void expandLeafNodes(ClinicalEntityType cdt,
                                                       Map<String, EntityCriterion> leaves, EHRDataSource dataSource) {
        leaves.forEach((node_id, criterion) -> {
            List<EntityValue> converted = new ArrayList<>();
            for (EntityValue v : criterion.components) {
                Set<EntityValue> convertedValues = dataSource.convertToLocalTerminology(cdt, v);
                converted.add(new SynonymExpandedEntityValues(convertedValues));
            }
            criterion.setComponents(converted.toArray(new EntityValue[0]));
            // TODO verify that modifying in-place will not break scoring
        });
    }
}
