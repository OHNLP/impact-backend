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
import org.ohnlp.ir.cat.connections.DataConnection;
import org.ohnlp.ir.cat.criteria.CompositeCriterion;
import org.ohnlp.ir.cat.criteria.Criterion;
import org.ohnlp.ir.cat.criteria.CriterionValue;
import org.ohnlp.ir.cat.ehr.datasource.EHRDataSource;
import org.ohnlp.ir.cat.scoring.BM25Scorer;
import org.ohnlp.ir.cat.scoring.Scorer;
import org.ohnlp.ir.cat.structs.ClinicalDataType;
import org.ohnlp.ir.cat.structs.PatientScore;
import org.ohnlp.ir.cat.temp.CriterionDefinitionDTO;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;

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
        CriterionDefinitionDTO criterion = middleware.getForObject(
                "/_projects/criterion?project_uid={project}",
                CriterionDefinitionDTO.class,
                Map.of("project_uid", jobUID.toString().toUpperCase(Locale.ROOT)));
        Criterion localCriterion = mapAPICriterionToBackendLogics(criterion);
        Map<ClinicalDataType, Map<String, CriterionValue>> leafsByDataType = getLeafsByDataType(localCriterion);
        // Now actually run the pipeline
        Pipeline p = Pipeline.create();
        Scorer scorer = new BM25Scorer(); // TODO this should be configurable
        // Iterate through all data types, generating scores for all leaf nodes
        List<PCollection<KV<KV<String, String>, PatientScore>>> leafScoreList = new ArrayList<>();
        for (Map.Entry<ClinicalDataType, Map<String, CriterionValue>> e : leafsByDataType.entrySet()) {
            ClinicalDataType cdt = e.getKey();
            Map<String, CriterionValue> leaves = e.getValue();
            Map<String, Set<CriterionValue>> ehrExpandedLeafNodes = expandLeafNodes(cdt, leaves, ehrDataSource);
            // Score based on EHR leaf nodes
            PCollection<KV<KV<String, String>, PatientScore>> ehrScores = scorer.score(p, ehrExpandedLeafNodes, cdt, ehrDataSource);
            // TODO combine with nlp results here
            leafScoreList.add(ehrScores);
        }
        // Union all the leaf scores across the disparate clinical data types
        PCollection<KV<KV<String, String>, PatientScore>> leafScores = PCollectionList.of(leafScoreList).apply(Flatten.pCollections());
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
                                                localCriterion.score(leafScores)
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


    private static Object instantiateZeroArgumentConstructorClass(String clazz)
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        return Class.forName(clazz).getDeclaredConstructor().newInstance();
    }

    private static Criterion mapAPICriterionToBackendLogics(CriterionDefinitionDTO criterion) {
        return null; // TODO
    }

    private static Map<ClinicalDataType, Map<String, CriterionValue>> getLeafsByDataType(Criterion criterion) {
        Map<ClinicalDataType, Map<String, CriterionValue>> ret = new HashMap<>();
        getLeafsByDataTypeRecurs(criterion, ret);
        return ret;
    }

    private static void getLeafsByDataTypeRecurs(Criterion criterion, Map<ClinicalDataType, Map<String, CriterionValue>> leafsByCDT) {
        if (criterion instanceof CriterionValue) { // Leaf Node
            ClinicalDataType type = ((CriterionValue)criterion).getType();
            leafsByCDT.computeIfAbsent(type, k -> new HashMap<>())
                    .put(criterion.getCriterionUID().toString().toUpperCase(Locale.ROOT), (CriterionValue) criterion);
        } else if (criterion instanceof CompositeCriterion) {
            for (Criterion child : ((CompositeCriterion)criterion).getSubcriterion()) {
                getLeafsByDataTypeRecurs(child, leafsByCDT);
            }
        } else {
            throw new UnsupportedOperationException("Unknown criterion type that is not a leaf or a composite");
        }
    }

    private static Map<String, Set<CriterionValue>> expandLeafNodes(ClinicalDataType cdt,
            Map<String, CriterionValue> leaves, EHRDataSource dataSource) {
        Map<String, Set<CriterionValue>> ret = new HashMap<>();
        leaves.forEach((node_id, criterion) -> {
            if (!criterion.getFieldName().endsWith("code")) { // TODO handle this more elegantly (through enum switch maybe?)
                ret.put(node_id, Collections.singleton(criterion));
            } else {
                ret.put(node_id, dataSource.convertToLocalTerminology(cdt, criterion));
            }
        });
        return ret;
    }
}
