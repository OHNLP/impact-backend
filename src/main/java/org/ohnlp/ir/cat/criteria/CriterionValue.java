package org.ohnlp.ir.cat.criteria;


import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.ir.cat.structs.ClinicalDataType;
import org.ohnlp.ir.cat.structs.PatientScore;
import org.ohnlp.ir.cat.temp.ClinicalEntityType;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CriterionValue extends Criterion implements Serializable {
    private ClinicalDataType type;
    private String fieldName;
    private String[] values;
    private Relation reln;
    private transient FhirContext internalContext = FhirContext.forR4Cached();
    private transient ThreadLocal<SimpleDateFormat> sdf;
    private transient ThreadLocal<ObjectMapper> om;


    public CriterionValue() {
        this.sdf = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
        this.om = ThreadLocal.withInitial(ObjectMapper::new);
    }

    public ClinicalDataType getType() {
        return type;
    }

    @Override
    public boolean matches(DomainResource resource) {
        String resourceJSON = internalContext.newJsonParser().encodeResourceToString(resource);
        LinkedList<String> pathStack = new LinkedList<>(Arrays.asList(fieldName.split("\\.")));
        List<String> valueList = new ArrayList<>();
        try {
            JsonNode json = om.get().readTree(resourceJSON);
            findValuesFromJsonPath(json, pathStack, valueList);
        } catch (JsonProcessingException e) {
            return false;
        }
        for (String value : valueList) {
            if (coerceAndCompare(value)) {
                return true;
            }
        }
        return false;
    }

    private void findValuesFromJsonPath(JsonNode json, LinkedList<String> pathStack, List<String> valueList) {
        // Just simply always iterate if array
        if (json instanceof ArrayNode) {
            for (JsonNode child : json) {
                findValuesFromJsonPath(child, pathStack, valueList);
            }
        }
        if (pathStack.size() > 0) {
            String first = pathStack.removeFirst();
            if (json.has(first)) {
                findValuesFromJsonPath(json.get(first), pathStack, valueList);
            }
        } else {
            // Return current JSON value
            valueList.add(json.asText());
        }
    }

    private boolean coerceAndCompare(String value) {
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
        if (reln.equals(Relation.IN)) {
            return Arrays.stream(values).map(String::toLowerCase).collect(Collectors.toSet()).contains(value.toLowerCase(Locale.ROOT));
        } else if (reln.equals(Relation.EQ)) {
            return value.equalsIgnoreCase(values[0]); // TODO there might be some value in allowing for case sensitive matches
        } else {
            throw new UnsupportedOperationException("Cannot execute " + reln + " on undefined/string datatype");
        }
    }

    @Override
    public double score(Map<String, PatientScore> scoreByCriterionUID) {
        return scoreByCriterionUID.containsKey(criterionUID) ? scoreByCriterionUID.get(criterionUID).getScore() : 0.00;
    }

    private boolean compareNumeric(double input) throws NumberFormatException {
        double val1 = Double.parseDouble(values[0]);
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
                double val2 = Double.parseDouble(values[1]);
                return input >= val1 && input < val2;
            case IN:
                Set<Double> valueset = new HashSet<>();
                Arrays.stream(values).mapToDouble(Double::parseDouble).forEach(valueset::add);
                return valueset.contains(input);
        }
        return false;
    }

    private boolean compareDates(Date input) throws ParseException {
        Date val1 = sdf.get().parse(values[0]);
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
                Date val2 = sdf.get().parse(values[1]);
                return input.getTime() >= val1.getTime() && input.getTime() < val2.getTime();
            case IN:
                Set<Long> valueset = new HashSet<>();
                Arrays.stream(values).flatMap(s -> {
                    try {
                        return Stream.of(sdf.get().parse(s).getTime());
                    } catch (ParseException e) {
                        return Stream.empty();
                    }
                }).forEach(valueset::add);
                return valueset.contains(input.getTime());
        }
        return false;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }

    public Relation getReln() {
        return reln;
    }

    public void setReln(Relation reln) {
        this.reln = reln;
    }
}
