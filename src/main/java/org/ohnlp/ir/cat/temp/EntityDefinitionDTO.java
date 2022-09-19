package org.ohnlp.ir.cat.temp;

public class EntityDefinitionDTO {
    public ClinicalEntityType type;
    public ValueDefinitionDTO[] definitionComponents;

    public ClinicalEntityType getType() {
        return type;
    }

    public void setType(ClinicalEntityType type) {
        this.type = type;
    }

    public ValueDefinitionDTO[] getDefinitionComponents() {
        return definitionComponents;
    }

    public void setDefinitionComponents(ValueDefinitionDTO[] definitionComponents) {
        this.definitionComponents = definitionComponents;
    }
}
