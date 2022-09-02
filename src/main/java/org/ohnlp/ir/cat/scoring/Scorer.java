package org.ohnlp.ir.cat.scoring;

import org.ohnlp.ir.cat.ehr.datasource.EHRDataSource;
import org.ohnlp.ir.cat.structs.ClinicalDataType;

public abstract class Scorer {

    protected final EHRDataSource dataSource;
    protected final ClinicalDataType queryType;

    public Scorer(EHRDataSource dataSource, ClinicalDataType queryType) {
        this.dataSource = dataSource;
        this.queryType = queryType;
    }
}
