package org.ohnlp.ir.cat.ehr.datasource;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.hl7.fhir.r4.model.*;
import org.ohnlp.ir.cat.DataConnection;
import org.ohnlp.ir.cat.connections.BigQueryDataConnectionImpl;
import org.ohnlp.ir.cat.criteria.CriterionValue;
import org.ohnlp.ir.cat.structs.ClinicalDataType;

import java.util.*;

public class OHDSICDMDataSource implements EHRDataSource {

    private DataConnection ehrDataConnection;
    private String cdmSchemaName;

    @Override
    public PCollection<Person> getPersons(Pipeline pipeline) {
        return ehrDataConnection.getForQueryAndSchema(
                pipeline,
                "SELECT person_id, " +
                        "gender_concept_id, " +
                        "year_of_birth, " +
                        "month_of_birth," +
                        "day_of_birth," +
                        "race_concept_id," +
                        "ethnicity_concept_id" +
                        " FROM " + cdmSchemaName + ".person ",
                Schema.builder()
                        .addFields(
                                Schema.Field.of("person_id", Schema.FieldType.INT64),
                                Schema.Field.of("gender_concept_id", Schema.FieldType.INT32),
                                Schema.Field.of("year_of_birth", Schema.FieldType.INT32),
                                Schema.Field.of("month_of_birth", Schema.FieldType.INT32),
                                Schema.Field.of("day_of_birth", Schema.FieldType.INT32),
                                Schema.Field.of("race_concept_id", Schema.FieldType.INT32),
                                Schema.Field.of("ethnicity_concept_id", Schema.FieldType.INT32)
                        ).build()
        ).apply("Convert Person Read to FHIR", ParDo.of(
                new DoFn<Row, Person>() {
                    @ProcessElement
                    public void process(@Element Row in, OutputReceiver<Person> out) {
                        String personID = in.getInt64("person_id") + "";
                        int genderConceptId = in.getInt32("gender_concept_id");
                        int birthyr = in.getInt32("year_of_birth");
                        int birthmnth = in.getInt32("month_of_birth");
                        int birthday = in.getInt32("day_of_birth");
                        int raceConceptId = in.getInt32("race_concept_id");
                        int ethnicityConceptId = in.getInt32("ethnicity_concept_id");
                        Person p = new Person();
                        p.setId(personID);
                        switch (genderConceptId) {
                            case 0:
                                p.setGender(Enumerations.AdministrativeGender.NULL);
                                break;
                            case 8507:
                                p.setGender(Enumerations.AdministrativeGender.MALE);
                                break;
                            case 8532:
                                p.setGender(Enumerations.AdministrativeGender.FEMALE);
                                break;
                            default:
                                p.setGender(Enumerations.AdministrativeGender.UNKNOWN);
                                break;
                        }
                        p.setGender(genderConceptId == 0 ? // Only accepted values are 8507/8532, leave 0 as null fallback
                                Enumerations.AdministrativeGender.NULL :
                                (genderConceptId == 8507 ?
                                        Enumerations.AdministrativeGender.MALE :
                                        Enumerations.AdministrativeGender.FEMALE));
                        p.setBirthDate(new GregorianCalendar(birthyr, birthmnth - 1, birthday).getTime());
                        // TODO seems race and ethnicity not mapped to FHIR person? investigate where else this is.
                        out.output(p);
                    }
                }
        ));
    }

    @Override
    public PCollection<Condition> getConditions(Pipeline pipeline) {
        return ehrDataConnection.getForQueryAndSchema(
                pipeline,
                "SELECT condition_occurrence_id, " +
                        "person_id, " +
                        "condition_concept_id, " +
                        "condition_start_date FROM " + cdmSchemaName + ".condition_occurrence ",
                Schema.builder()
                        .addFields(
                                Schema.Field.of("condition_occurrence_id", Schema.FieldType.INT64),
                                Schema.Field.of("person_id", Schema.FieldType.INT64),
                                Schema.Field.of("condition_concept_id", Schema.FieldType.INT32),
                                Schema.Field.of("condition_start_date", Schema.FieldType.DATETIME)
                        ).build()
        ).apply("Convert Condition Read to FHIR", ParDo.of(
                new DoFn<Row, Condition>() {
                    @ProcessElement
                    public void process(@Element Row in, OutputReceiver<Condition> out) {
                        String recordID = in.getInt64("condition_occurrence_id") + "";
                        String personID = in.getInt64("person_id") + "";
                        String conditionConceptID = in.getInt32("condition_concept_id") + "";
                        Date dtm = new Date(in.getDateTime("condition_start_date").getMillis());
                        Condition cdn = new Condition();
                        cdn.setId(recordID);
                        cdn.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
                        cdn.setCode(
                                new CodeableConcept().addCoding(
                                        new Coding(
                                                "https://athena.ohdsi.org/",
                                                conditionConceptID,
                                                "Autogenerated OHDSI Mapping")) // TODO better mapping/descriptions
                        );
                        cdn.setRecordedDate(dtm);
                        out.output(cdn);
                    }
                }
        ));
    }

    @Override
    public PCollection<MedicationStatement> getMedications(Pipeline pipeline) {
        return ehrDataConnection.getForQueryAndSchema(
                pipeline,
                "SELECT drug_exposure_id, " +
                        "person_id, " +
                        "drug_concept_id, " +
                        "drug_exposure_start_date," +
                        "drug_exposure_end_date FROM " + cdmSchemaName + ".drug_exposure ",
                Schema.builder()
                        .addFields(
                                Schema.Field.of("drug_exposure_id", Schema.FieldType.INT64),
                                Schema.Field.of("person_id", Schema.FieldType.INT64),
                                Schema.Field.of("drug_concept_id", Schema.FieldType.INT32),
                                Schema.Field.of("drug_exposure_start_date", Schema.FieldType.DATETIME),
                                Schema.Field.of("drug_exposure_end_date", Schema.FieldType.DATETIME)
                        ).build()
        ).apply("Convert Drug Exposure Read to FHIR", ParDo.of(
                new DoFn<Row, MedicationStatement>() {
                    @ProcessElement
                    public void process(@Element Row in, OutputReceiver<MedicationStatement> out) {
                        String recordID = in.getInt64("drug_exposure_id") + "";
                        String personID = in.getInt64("person_id") + "";
                        String drugConceptId = in.getInt32("drug_concept_id") + "";
                        Date dtm = new Date(in.getDateTime("drug_exposure_start_date").getMillis());
                        // TODO see about mapping date ends? there doesn't seem to currently be a target in FHIR somehow (or am just blind)
                        MedicationStatement ms = new MedicationStatement();
                        ms.setId(recordID);
                        ms.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
                        ms.setMedication(
                                new CodeableConcept().addCoding(
                                        new Coding(
                                                "https://athena.ohdsi.org/",
                                                drugConceptId,
                                                "Autogenerated OHDSI Mapping")) // TODO
                        );
                        ms.setDateAsserted(dtm);
                        out.output(ms);
                    }
                }
        ));
    }

    @Override
    public PCollection<Procedure> getProcedures(Pipeline pipeline) {
        return ehrDataConnection.getForQueryAndSchema(
                pipeline,
                "SELECT procedure_occurrence_id, " +
                        "person_id, " +
                        "procedure_concept_id, " +
                        "procedure_date FROM " + cdmSchemaName + ".procedure_occurrence",
                Schema.builder()
                        .addFields(
                                Schema.Field.of("procedure_occurrence_id", Schema.FieldType.INT64),
                                Schema.Field.of("person_id", Schema.FieldType.INT64),
                                Schema.Field.of("procedure_concept_id", Schema.FieldType.INT32),
                                Schema.Field.of("procedure_date", Schema.FieldType.DATETIME)
                        ).build()
        ).apply("Convert Procedure Read to FHIR", ParDo.of(
                new DoFn<Row, Procedure>() {
                    @ProcessElement
                    public void process(@Element Row in, OutputReceiver<Procedure> out) {
                        String recordID = in.getInt64("procedure_occurrence_id") + "";
                        String personID = in.getInt64("person_id") + "";
                        String conceptID = in.getInt32("procedure_concept_id") + "";
                        Date dtm = new Date(in.getDateTime("procedure_date").getMillis());
                        Procedure prc = new Procedure();
                        prc.setId(recordID);
                        prc.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
                        prc.setCode(
                                new CodeableConcept().addCoding(
                                        new Coding(
                                                "https://athena.ohdsi.org/",
                                                conceptID,
                                                "Autogenerated OHDSI Mapping")) // TODO better mapping/descriptions
                        );
                        prc.setPerformed(new DateTimeType(dtm));
                        out.output(prc);
                    }
                }
        ));
    }

    @Override
    public PCollection<Observation> getObservations(Pipeline pipeline) {
        return ehrDataConnection.getForQueryAndSchema(
                pipeline,
                "SELECT observation_id, " +
                        "person_id, " +
                        "observation_concept_id, " +
                        "observation_date," +
                        "value_as_number," +
                        "value_as_string FROM " + cdmSchemaName + ".procedure_occurrence",
                Schema.builder()
                        .addFields(
                                Schema.Field.of("observation_id", Schema.FieldType.INT64),
                                Schema.Field.of("person_id", Schema.FieldType.INT64),
                                Schema.Field.of("observation_concept_id", Schema.FieldType.INT32),
                                Schema.Field.of("observation_date", Schema.FieldType.DATETIME),
                                Schema.Field.of("value_as_number", Schema.FieldType.FLOAT),
                                Schema.Field.of("value_as_string", Schema.FieldType.STRING)
                        ).build()
        ).apply("Convert Observation Read to FHIR", ParDo.of(
                new DoFn<Row, Observation>() {
                    @ProcessElement
                    public void process(@Element Row in, OutputReceiver<Observation> out) {
                        String recordID = in.getInt64("observation_id") + "";
                        String personID = in.getInt64("person_id") + "";
                        String conceptID = in.getInt32("observation_concept_id") + "";
                        Date dtm = new Date(in.getDateTime("observation_date").getMillis());
                        Observation obs = new Observation();
                        obs.setId(recordID);
                        obs.setSubject(new Reference().setIdentifier(new Identifier().setValue(personID)));
                        obs.setCode(
                                new CodeableConcept().addCoding(
                                        new Coding(
                                                "https://athena.ohdsi.org/",
                                                conceptID,
                                                "Autogenerated OHDSI Mapping")) // TODO better mapping/descriptions
                        );
                        String value = null;
                        if (in.getFloat("value_as_number") != null) {
                            value = in.getFloat("value_as_number") + "";
                        } else {
                            value = in.getString("value_as_string");
                            if (value != null && value.trim().length() == 0) {
                                value = null;
                            }
                        }
                        if (value != null) {
                            obs.setValue(new StringType(value));
                        }
                        obs.setIssued(dtm);
                        out.output(obs);
                    }
                }
        ));
    }


    @Override
    public void loadConfig(JsonNode node) {
        // TODO
        this.cdmSchemaName = "cdm";
        this.ehrDataConnection = new BigQueryDataConnectionImpl();
    }

    @Override
    public Set<CriterionValue> convertToLocalTerminology(ClinicalDataType type, CriterionValue input) {
        return new HashSet<>(); // TODO
    }
}
