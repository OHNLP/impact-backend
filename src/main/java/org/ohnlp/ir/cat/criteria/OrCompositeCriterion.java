package org.ohnlp.ir.cat.criteria;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.ir.cat.structs.PatientScore;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class OrCompositeCriterion extends CompositeCriterion {

    private final int minMatch;

    public OrCompositeCriterion(int minMatch, Criterion... subcriterion) {
        super(subcriterion);
        this.minMatch = minMatch;
    }

    @Override
    public boolean matches(DomainResource resource) {
        int match = 0;
        for (Criterion val : subcriterion) {
            if (val.matches(resource)) {
                match++;
                if (match == minMatch) {
                    return true;
                }
            }
        }
        return false;
    }

    // For OR, return max of subscores. For minMatch > 1, use average of top minMatch subscores (thus penalizing if
    // minimum number not met)
    @Override
    public double score(Map<String, PatientScore> scoreByCriterionUID) {
        LinkedList<Double> scoresSorted = Arrays.stream(subcriterion).map(c -> c.score(scoreByCriterionUID))
                .sorted()
                .collect(Collectors.toCollection(LinkedList::new));
        double scoreTotal = 0;
        int scoresContributedCount = 0;
        while (scoresContributedCount < minMatch) {
            scoreTotal += scoresSorted.size() > 0 ? scoresSorted.removeLast() : 0;
            scoresContributedCount++;
        }
        return scoreTotal / minMatch;
    }
}
