package org.ohnlp.ir.cat.scoring;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Join;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.*;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.ir.cat.criteria.CriterionValue;
import org.ohnlp.ir.cat.ehr.datasource.EHRDataSource;
import org.ohnlp.ir.cat.structs.ClinicalDataType;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.Map;
import java.util.Set;

/**
 * Scores an input set of query terms with BM25 scoring.
 */
// TODO cleanup inline schema declarations
public class BM25Scorer extends Scorer {

    // BM25 Hyperparameters
    private double k1 = 1.2;
    private double b = 0.75;

    // Patient ID Extraction Functions TODO consider using reflection instead since field names/types is same even if not abstractified
    private static final SerializableFunction<DomainResource, String>
            CONDITION_PATUID_EXTRACTION = (r) -> ((Condition) r).getSubject().getIdentifier().getValue();
    private static final SerializableFunction<DomainResource, String>
            PROCEDURE_PATUID_EXTRACTION = (r) -> ((Procedure) r).getSubject().getIdentifier().getValue();
    private static final SerializableFunction<DomainResource, String>
            MEDICATION_PATUID_EXTRACTION = (r) -> ((MedicationStatement) r).getSubject().getIdentifier().getValue();
    private static final SerializableFunction<DomainResource, String>
            OBSERVATION_PATUID_EXTRACTION = (r) -> ((Observation) r).getSubject().getIdentifier().getValue();

    public BM25Scorer(EHRDataSource dataSource,
                      ClinicalDataType queryType) {
        super(dataSource, queryType);
    }


    public PCollection<KV<String, PatientScore>> score(Pipeline p, Map<String, Set<CriterionValue>> query) {
        PCollection<? extends DomainResource> items;
        SerializableFunction<DomainResource, String> patUIDExtractorFn;
        switch (this.queryType) {
            case CONDITION:
                items = this.dataSource.getConditions(p);
                patUIDExtractorFn = CONDITION_PATUID_EXTRACTION;
                break;
            case PROCEDURE:
                items = this.dataSource.getProcedures(p);
                patUIDExtractorFn = PROCEDURE_PATUID_EXTRACTION;
                break;
            case MEDICATION:
                items = this.dataSource.getMedications(p);
                patUIDExtractorFn = MEDICATION_PATUID_EXTRACTION;
                break;
            case OBSERVATION:
                items = this.dataSource.getObservations(p);
                patUIDExtractorFn = OBSERVATION_PATUID_EXTRACTION;
                break;
            default:
                throw new UnsupportedOperationException("Unknown query type " + queryType);
        }

        // Extract patient UIDs from DomainResource objects
        PCollection<KV<String, DomainResource>> allRecordsByPatientUID = items.apply(
                "Data Preprocessing: Split into (PatientUID, DomainResource) pairs",
                ParDo.of(
                        new DoFn<DomainResource, KV<String, DomainResource>>() {
                            @ProcessElement
                            public void process(@Element DomainResource record,
                                                       OutputReceiver<KV<String, DomainResource>> out) {
                                out.output(KV.of(patUIDExtractorFn.apply(record), record));
                            }
                        }
                )
        );

        // Apply cohort definition criteria matching, produces <criteriaUID, <patientUID, matchingResource>>
        PCollection<KV<String, KV<String, DomainResource>>> queryMatches = allRecordsByPatientUID.apply(
                "Query: Filter table to Criteria matching items",
                ParDo.of(
                        new DoFn<KV<String, DomainResource>, KV<String, KV<String, DomainResource>>>() {
                            // TODO: match against query param using FHIR-based values
                        }
                )
        );

        // Calculate BM25 Parameter (Collection Size/Number of Patients)
        PCollectionView<Long> collSizeView = getNumPatientsInEHR(allRecordsByPatientUID).apply(View.asSingleton());
        // Calculate document lengths (number of records per patient)
        PCollection<KV<String, Long>> doclen = allRecordsByPatientUID.apply(
                "BM25 Step 2.1: Count records by Patient ID",
                Count.perKey()
        );
        // Calculate BM25 Parameter (Average Document Length/Average Number of Records per Patient of given Data Type)
        PCollectionView<Double> avgDocLen = getAverageNumRecordsPerPatient(doclen).apply(View.asSingleton());


        // Calculate Inverse Document Frequency
        Schema idfSchema = Schema.of(
                Schema.Field.of("criterion_uid", Schema.FieldType.STRING),
                Schema.Field.of("idf", Schema.FieldType.INT64)
        );

        PCollection<Row> idf = getIdf(idfSchema, queryMatches, collSizeView);

        // Calculate term frequency per criterion
        Schema tfSchema = Schema.of(
                Schema.Field.of("criterion_uid", Schema.FieldType.STRING),
                Schema.Field.of("patient_uid", Schema.FieldType.STRING),
                Schema.Field.of("tf", Schema.FieldType.INT64)
        );

        PCollection<Row> tf = getTF(tfSchema, queryMatches);

        // Calculate bm25 joining tf, idf, and docLen PCollections
        // Because of beam limitations, join structure is as follows.
        // Doc lens can be accessed at "rhs"
        // idf can be accessed at lhs -> rhs
        // tf can be accessed at lhs -> lhs
        return getScores(doclen, idf, tf, avgDocLen);
    }

    private PCollection<Row> getIdf(Schema idfSchema, PCollection<KV<String, KV<String, DomainResource>>> queryMatches, PCollectionView<Long> collSizeView) {
        return queryMatches.apply(
                "BM25 Step 3.1 (IDF): Reformat into (criterionUID, patientUID) pairs", ParDo.of(
                        new DoFn<KV<String, KV<String, DomainResource>>, KV<String, String>>() {
                            @ProcessElement
                            public void process(@Element KV<String, KV<String, DomainResource>> record,
                                                       OutputReceiver<KV<String, String>> out) {
                                out.output(KV.of(record.getKey(), record.getValue().getKey()));
                            }
                        }
                )
        ).apply(
                "BM25 Step 3.2 (IDF): Get distinct (criterionUID, patientUID) pairs",
                Distinct.withRepresentativeValueFn((kv) -> kv.getKey() + "|" + kv.getValue())
        ).apply(
                "BM25 Step 3.3 (IDF):Get number of patients per criterion",
                Count.perKey()
        ).apply("BM25 Step 3.4 (IDF): Calculate values",
                ParDo.of(
                        new DoFn<KV<String, Long>, KV<String, Double>>() {
                            @ProcessElement
                            public void process(ProcessContext c, @Element KV<String, Long> e, OutputReceiver<KV<String, Double>> out) {
                                long collSize = c.sideInput(collSizeView);
                                out.output(KV.of(e.getKey(), idf(collSize, e.getValue())));
                            }
                        }
                ).withSideInputs(collSizeView)
        ).apply(ParDo.of(new DoFn<KV<String, Double>, Row>() {
            @ProcessElement
            public void process(@Element KV<String, Double> in, OutputReceiver<Row> out) {
                out.output(Row.withSchema(idfSchema).addValues(in.getKey(), in.getValue()).build());
            }
        }));
    }

    private PCollection<Long> getNumPatientsInEHR(PCollection<KV<String, DomainResource>> allRecordsByPatientUID) {
        return allRecordsByPatientUID.apply(
                "BM25 Step 1: Calculate Collection Size",
                new PTransform<PCollection<KV<String, DomainResource>>, PCollection<Long>>() {
                    @Override
                    public PCollection<Long> expand(PCollection<KV<String, DomainResource>> input) {
                        return input.apply(
                                "BM25 Step 1.1: Extract Patient UIDs",
                                ParDo.of(new DoFn<KV<String, DomainResource>, String>() {
                                    @ProcessElement
                                    public void process(@Element KV<String, DomainResource> e, OutputReceiver<String> out) {
                                        out.output(e.getKey());
                                    }
                                })
                        ).apply(
                                "BM25 Step 1.2: Dedup Patient UIDs",
                                Distinct.create()
                        ).apply(
                                "BM25 Step 1.3: Count Dedup'ed Patient UIDs",
                                Count.globally()
                        );
                    }
                }
        );
    }

    private PCollection<Double> getAverageNumRecordsPerPatient(PCollection<KV<String, Long>> allRecordsByPatientUID) {
        return allRecordsByPatientUID.apply(
                "BM25 Step 2: Calculate Average Collection Length",
                new PTransform<PCollection<KV<String, Long>>, PCollection<Double>>() {
                    @Override
                    public PCollection<Double> expand(PCollection<KV<String, Long>> input) {
                        return input.apply(
                                "BM25 Step 2.2: Extract counts",
                                ParDo.of(
                                        new DoFn<KV<String, Long>, Long>() {
                                            @ProcessElement
                                            public void process(@Element KV<String, Long> e, OutputReceiver<Long> out) {
                                                out.output(e.getValue());
                                            }
                                        }
                                )
                        ).apply(
                                "BM25 Step 3.3: Calculate Mean",
                                Mean.globally()
                        );
                    }
                }
        );
    }


    private PCollection<Row> getTF(Schema tfSchema, PCollection<KV<String, KV<String, DomainResource>>> queryMatches) {
        return queryMatches.apply(
                "BM25 Step 4: Get Term Frequency per Patient (Number of Records Matching Criteria per Patient",
                new PTransform<PCollection<KV<String, KV<String, DomainResource>>>, PCollection<KV<KV<String, String>, Long>>>() {
                    @Override
                    public PCollection<KV<KV<String, String>, Long>> expand(PCollection<KV<String, KV<String, DomainResource>>> input) {
                        return input.apply(
                                ParDo.of(
                                        new DoFn<KV<String, KV<String, DomainResource>>, KV<KV<String, String>, Integer>>() {
                                            // Map to <<CriteriaUID, PatientUID>, 1> so we can just count by key
                                            @ProcessElement
                                            public void process(
                                                    @Element KV<String, KV<String, DomainResource>> in,
                                                    OutputReceiver<KV<KV<String, String>, Integer>> out
                                            ) {
                                                out.output(KV.of(KV.of(in.getKey(), in.getValue().getKey()), 1));
                                            }
                                        }
                                )
                        ).apply(
                                Count.perKey()
                        );
                    }
                }
        ).apply(
                ParDo.of(
                        new DoFn<KV<KV<String, String>, Long>, KV<String, KV<String, Long>>>() {
                            // Map back to correct format
                            @ProcessElement
                            public void process(
                                    @Element KV<KV<String, String>, Long> in,
                                    OutputReceiver<KV<String, KV<String, Long>>> out
                            ) {
                                out.output(KV.of(in.getKey().getKey(), KV.of(in.getKey().getValue(), in.getValue())));
                            }
                        }
                )
        ).apply(ParDo.of(new DoFn<KV<String, KV<String, Long>>, Row>() {
            @ProcessElement
            public void process(@Element KV<String, KV<String, Long>> in, OutputReceiver<Row> out) {
                out.output(Row.withSchema(tfSchema).addValues(in.getKey(), in.getValue().getKey(), in.getValue().getValue()).build());
            }
        }));
    }

    private PCollection<KV<String, PatientScore>> getScores(PCollection<KV<String, Long>> doclen, PCollection<Row> idf, PCollection<Row> tf, PCollectionView<Double> avgDocLen) {
        return tf.apply(
                "BM25 Step 5.1: Join TF, IDF, and docLen for calculation",
                // Use broadcast joins for efficiency since lhs is always smaller than rhs
                new PTransform<PCollection<Row>, PCollection<Row>>() {
                    @Override
                    public PCollection<Row> expand(PCollection<Row> inputTF) {
                        return Join.innerBroadcastJoin(doclen.apply(
                                ParDo.of(new DoFn<KV<String, Long>, Row>() {
                                    @ProcessElement
                                    public void process(@Element KV<String, Long> in, OutputReceiver<Row> out) {
                                        out.output(Row.withSchema(
                                                Schema.of(
                                                        Schema.Field.of("patient_uid", Schema.FieldType.STRING),
                                                        Schema.Field.of("doc_len", Schema.FieldType.INT64)
                                                )
                                        ).addValues(in.getKey(), in.getValue()).build());
                                    }
                                })
                        )).on(Join.FieldsEqual.left("rhs.patient_uid").right("patient_uid")).expand(
                                Join.innerBroadcastJoin(idf).using("criterion_uid").expand(inputTF)
                        );
                    }
                }
        ).apply(
                "BM25 Step 5.2: Calculate BM25 scores from joined values",
                ParDo.of(
                        new DoFn<Row, KV<String, PatientScore>>() {
                            // Technically row gets can produce NPEs but would never happen due to inner join by definition
                            @SuppressWarnings("ConstantConditions")
                            // TODO find better way to handle/rename nested joined items to something less confusing
                            @ProcessElement
                            public void process(ProcessContext c, @Element Row in, OutputReceiver<KV<String, PatientScore>> out) {
                                double bm25 = bm25(
                                        in.getRow("lhs").getRow("rhs").getInt64("idf").doubleValue(),
                                        in.getRow("lhs").getRow("lhs").getInt64("tf").doubleValue(),
                                        in.getRow("rhs").getInt64("doc_len").doubleValue(),
                                        c.sideInput(avgDocLen)
                                );
                                out.output(
                                        KV.of(
                                                in.getRow("lhs").getRow("lhs").getString("criterion_uid"),
                                                new PatientScore(
                                                        in.getRow("lhs").getRow("lhs").getString("patient_uid"),
                                                        bm25
                                                )
                                        )
                                );
                            }
                        }
                ).withSideInputs(avgDocLen)
        );
    }

    public double bm25(double idf, double tf, double docLen, double avgDocLen) {
        return idf * (tf * (k1 + 1)) / (tf + (k1 * (1 - b + (b * (docLen / avgDocLen)))));
    }

    private double idf(long collsize, double numDocsWithTerm) {
        return Math.log(((collsize - numDocsWithTerm + 0.5) / (numDocsWithTerm + 0.5)) + 1);
    }
}
