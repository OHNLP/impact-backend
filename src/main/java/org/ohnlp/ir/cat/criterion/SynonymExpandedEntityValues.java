package org.ohnlp.ir.cat.criterion;

import org.hl7.fhir.r4.model.DomainResource;
import org.ohnlp.cat.api.criteria.EntityValue;

import java.io.Serializable;
import java.util.Collection;

public class SynonymExpandedEntityValues extends EntityValue implements Serializable {
    private Collection<EntityValue> synonyms;

    public SynonymExpandedEntityValues() {}

    public SynonymExpandedEntityValues(Collection<EntityValue> synonyms) {
        this.synonyms = synonyms;
    }

    public Collection<EntityValue> getSynonyms() {
        return synonyms;
    }

    public void setSynonyms(Collection<EntityValue> synonyms) {
        this.synonyms = synonyms;
    }

    @Override
    public boolean matches(DomainResource resource) {
        for (EntityValue child : synonyms) {
            if (!child.matches(resource)) {
                return true;
            }
        }
        return false;
    }
}
