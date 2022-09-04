package org.ohnlp.ir.cat.structs;

import java.io.Serializable;

public class PatientScore implements Serializable {
    private String patientUID;
    private Double score;

    public PatientScore() {}

    public PatientScore(String patientUID, Double score) {
        this.patientUID = patientUID;
        this.score = score;
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
}
