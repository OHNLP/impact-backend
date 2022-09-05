package org.ohnlp.ir.cat.criteria;


import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hl7.fhir.r4.model.DomainResource;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;

public class CriterionValue extends Criterion implements Serializable {
    private String fieldName;
    private String value1;
    private String value2;
    private Relation reln;
    private FhirContext internalContext = FhirContext.forR4Cached();
    private transient ThreadLocal<SimpleDateFormat> sdf;
    private transient ThreadLocal<ObjectMapper> om;


    public CriterionValue() {
        this.sdf = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
        this.om = ThreadLocal.withInitial(ObjectMapper::new);
    }

    @Override
    public boolean matches(DomainResource resource) {
        String resourceJSON = internalContext.newJsonParser().encodeResourceToString(resource);
        String value;
        try {
            JsonNode json = om.get().readTree(resourceJSON);
            LinkedList<String> pathStack = new LinkedList<>(Arrays.asList(fieldName.split("\\.")));
            while (pathStack.size() > 1) {
                if (!json.has(pathStack.getFirst())) {
                    return false; // If path doesn't exist in resource return false.
                    // TODO should consider including value type to ensure proper matching/error otherwise instead of JSON-based impl
                }
                json = json.get(pathStack.getFirst());
                pathStack.removeFirst();
            }
            value = json.get(pathStack.getFirst()).asText();
        } catch (JsonProcessingException e) {
            return false;
        }
        // Navigate to
        // First try to see if value is numeric
        boolean inputTypeValid = false;
        try {
            return compareNumeric(Double.parseDouble(value));
        } catch (NumberFormatException ignored) {
        }
        // Next try date format, yyyy-MM-dd
        try {
            return compareDates(sdf.get().parse(value));
        } catch (ParseException ignored) {
        }
        // Finally, do a direct string compare
        return value.equalsIgnoreCase(value1); // TODO there might be some value in allowing for case sensitive matches
    }

    private boolean compareNumeric(double input) throws NumberFormatException {
        double val1 = Double.parseDouble(value1);
        switch (reln) {
            case LT:
                return input < val1;
            case LTE:
                return input <= val1;
            case GT:
                return input > val1;
            case GTE:
                return input >= val1;
            case EQ:
                return input == val1;
            case BETWEEN:
                double val2 = Double.parseDouble(value2);
                return input >= val1 && input < val2;
        }
        return false;
    }

    private boolean compareDates(Date input) throws ParseException {
        Date val1 = sdf.get().parse(value1);
        switch (reln) {
            case LT:
                return input.getTime() < val1.getTime();
            case LTE:
                return input.getTime() <= val1.getTime();
            case GT:
                return input.getTime() > val1.getTime();
            case GTE:
                return input.getTime() >= val1.getTime();
            case EQ:
                return input.getTime() == val1.getTime();
            case BETWEEN:
                Date val2 = sdf.get().parse(value2);
                return input.getTime() >= val1.getTime() && input.getTime() < val2.getTime();
        }
        return false;
    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(String value1) {
        this.value1 = value1;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
    }

    public Relation getReln() {
        return reln;
    }

    public void setReln(Relation reln) {
        this.reln = reln;
    }
}
