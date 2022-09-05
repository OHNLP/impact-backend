package org.ohnlp.ir.cat.structs;

import java.io.Serializable;
import java.util.Set;

public class PatientScore implements Serializable {
    private String patientUID;
    private Set<String> evidenceIDs;
    private Double score;

    public PatientScore() {}

    public PatientScore(String patientUID, Double score, Set<String> evidenceIDs) {
        this.patientUID = patientUID;
        this.score = score;
        this.evidenceIDs = evidenceIDs;
    }

    public String getPatientUID() {
        return patientUID;
    }

    public void setPatientUID(String patientUID) {
        this.patientUID = patientUID;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public Set<String> getEvidenceIDs() {
        return evidenceIDs;
    }

    public void setEvidenceIDs(Set<String> evidenceIDs) {
        this.evidenceIDs = evidenceIDs;
    }
}
